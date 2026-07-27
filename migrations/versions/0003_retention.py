from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX ix_outbox_tail
            ON outbox (stream_seq)
            INCLUDE (id, aggregate_id, event_type)
            WHERE stream_seq IS NOT NULL;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION outbox_published_notify() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('outbox_published', '');
            RETURN NULL;
        END;
        $$;
        """
    )
    
    op.execute(
        """
        CREATE TRIGGER outbox_published_trigger
            AFTER UPDATE OF stream_seq ON outbox
            FOR EACH STATEMENT
            EXECUTE FUNCTION outbox_published_notify();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS outbox_published_trigger ON outbox;")
    op.execute("DROP FUNCTION IF EXISTS outbox_published_notify();")
    op.execute("DROP INDEX IF EXISTS ix_outbox_tail;")
