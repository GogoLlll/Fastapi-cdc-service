from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SEQUENCE outbox_stream_seq AS BIGINT START 1;")
    op.execute("ALTER TABLE outbox ADD COLUMN stream_seq BIGINT;")

    op.execute(
        """
        CREATE UNIQUE INDEX ix_outbox_stream_seq
            ON outbox (stream_seq)
            WHERE stream_seq IS NOT NULL;
        """
    )

    op.execute(
        """
        ALTER TABLE outbox ADD CONSTRAINT ck_outbox_published_has_seq
            CHECK ((published_at IS NULL) = (stream_seq IS NULL));
        """
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE outbox DROP CONSTRAINT IF EXISTS ck_outbox_published_has_seq;"
    )

    op.execute("DROP INDEX IF EXISTS ix_outbox_stream_seq;")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS stream_seq;")
    op.execute("DROP SEQUENCE IF EXISTS outbox_stream_seq;")
