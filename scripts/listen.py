from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone

import websockets

RESET = "\033[0m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
BLUE = "\033[34m"

COLOURS = {
    "item.created": GREEN,
    "item.updated": YELLOW,
    "item.deleted": RED,
}


def clock() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


class Listener:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cursor = args.last_event_id
        self.received = 0
        self.duplicates = 0
        self.gaps: list[tuple[int, int]] = []
        self.reconnects = 0

    def url(self) -> str:
        params = [f"last_event_id={self.cursor}"]
        if self.args.aggregate_id:
            params.append(f"aggregate_id={self.args.aggregate_id}")
        if not self.args.replay:
            params.append("replay=false")
        return f"{self.args.url}/api/v1/stream?" + "&".join(params)

    def show_event(self, message: dict) -> None:
        seq = message["stream_seq"]

        if seq <= self.cursor:
            self.duplicates += 1
            print(
                f"{DIM}{clock()}  #{seq:<5} DUPLICATE, discarded "
                f"(cursor is {self.cursor}){RESET}"
            )
            return

        if seq > self.cursor + 1 and self.received:
            self.gaps.append((self.cursor + 1, seq - 1))
            print(f"{RED}{clock()}  GAP: {self.cursor + 1}..{seq - 1} never arrived{RESET}")

        lag = (datetime.now(timezone.utc) - parse_ts(message["occurred_at"])).total_seconds()
        colour = COLOURS.get(message["event_type"], "")
        data = message["data"]

        print(
            f"{clock()}  #{seq:<5} {colour}{message['event_type']:<13}{RESET} "
            f"{data['name']:<22} v{data['version']:<3} "
            f"value={data['value']:<6} {DIM}+{lag * 1000:.0f}ms{RESET}"
        )

        self.cursor = seq
        self.received += 1

    def show_control(self, message: dict) -> None:
        kind = message["type"]
        if kind == "ping":
            if self.args.verbose:
                print(f"{DIM}{clock()}  ping{RESET}")
            return
        detail = {k: v for k, v in message.items() if k != "type"}
        print(f"{BLUE}{clock()}  [{kind}] {detail}{RESET}")

    async def run(self) -> int:
        while True:
            try:
                url = self.url()
                print(f"{DIM}connecting: {url}{RESET}")
                async with websockets.connect(url, open_timeout=10) as ws:
                    print(f"{GREEN}connected, cursor = {self.cursor}{RESET}\n")
                    async for raw in ws:
                        message = json.loads(raw)
                        if "type" in message:
                            self.show_control(message)
                        else:
                            self.show_event(message)
            except websockets.ConnectionClosed as exc:
                print(f"\n{RED}closed: code={exc.code} reason={exc.reason!r}{RESET}")
                if exc.code == 4003:
                    print(
                        f"{RED}Cursor {self.cursor} is older than retention. "
                        f"Resynchronise via GET /api/v1/items, then restart "
                        f"with the newest stream_seq you see.{RESET}"
                    )
                    return 1
            except KeyboardInterrupt:
                break
            except Exception as exc:
                print(f"{RED}error: {exc!r}{RESET}")

            if not self.args.reconnect:
                break

            self.reconnects += 1
            print(f"{YELLOW}reconnecting in 1s (cursor stays at {self.cursor}){RESET}\n")
            await asyncio.sleep(1)

        self.summary()
        return 0

    def summary(self) -> None:
        print(
            f"\n{DIM}--- received {self.received}, duplicates {self.duplicates}, "
            f"gaps {len(self.gaps)}, reconnects {self.reconnects}, "
            f"cursor {self.cursor} ---{RESET}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Watch the event stream the way a correct client would."
    )
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument(
        "--last-event-id",
        type=int,
        default=0,
        dest="last_event_id",
        help="resume from this stream_seq; 0 means live only",
    )
    parser.add_argument("--aggregate-id", default=None, dest="aggregate_id")
    parser.add_argument("--no-replay", action="store_false", dest="replay")
    parser.add_argument(
        "--reconnect",
        action="store_true",
        help="reconnect automatically, keeping the cursor",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="show pings")
    args = parser.parse_args()
    args.url = args.url.replace("http://", "ws://").replace("https://", "wss://")
    return args


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(Listener(parse_args()).run()))
    except KeyboardInterrupt:
        print()
