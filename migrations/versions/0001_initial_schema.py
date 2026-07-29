from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE items (
            id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            name        TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 255),
            value       INTEGER     NOT NULL DEFAULT 0,
            version     INTEGER     NOT NULL DEFAULT 1,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX ix_items_created_at ON items (created_at DESC, id);")

    op.execute(
        """
        CREATE TABLE outbox (
            id            BIGSERIAL   PRIMARY KEY,
            aggregate_type TEXT       NOT NULL DEFAULT 'item',
            aggregate_id  UUID        NOT NULL,
            event_type    TEXT        NOT NULL
                          CHECK (event_type IN ('item.created', 'item.updated', 'item.deleted')),
            payload       JSONB       NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            published_at  TIMESTAMPTZ
        );
        """
    )

    op.execute(
        """
        CREATE INDEX ix_outbox_unpublished
            ON outbox (id)
            WHERE published_at IS NULL;
        """
    )

    op.execute("CREATE INDEX ix_outbox_aggregate ON outbox (aggregate_id, id);")

    op.execute(
        """
        CREATE INDEX ix_outbox_published_at
            ON outbox (published_at)
            WHERE published_at IS NOT NULL;
        """
    )

    op.execute(
        """
        CREATE FUNCTION outbox_notify() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('outbox_new', '');
            RETURN NULL;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER outbox_notify_trigger
            AFTER INSERT ON outbox
            FOR EACH STATEMENT
            EXECUTE FUNCTION outbox_notify();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS outbox_notify_trigger ON outbox;")
    op.execute("DROP FUNCTION IF EXISTS outbox_notify();")
    op.execute("DROP TABLE IF EXISTS outbox;")
    op.execute("DROP TABLE IF EXISTS items;")
