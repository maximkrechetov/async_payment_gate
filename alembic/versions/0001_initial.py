"""payments + outbox

Revision ID: 0001_initial
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


currency = postgresql.ENUM("RUB", "USD", "EUR", name="currency", create_type=False)
payment_status = postgresql.ENUM("pending", "succeeded", "failed", name="payment_status", create_type=False)
webhook_status = postgresql.ENUM("pending", "delivered", "failed", name="webhook_status", create_type=False)
outbox_status = postgresql.ENUM("pending", "published", name="outbox_status", create_type=False)


def upgrade() -> None:
    bind = op.get_bind()
    for t in (currency, payment_status, webhook_status, outbox_status):
        t.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("metadata", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("currency", currency, nullable=False),
        sa.Column("status", payment_status, nullable=False, server_default="pending"),
        sa.Column("webhook_status", webhook_status, nullable=False, server_default="pending"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("webhook_error", sa.Text(), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_payment_status"),
        sa.CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'failed')",
            name="ck_payments_webhook_status",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_payments"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status", "payments", ["status"])
    op.create_index("ix_payments_created_at", "payments", ["created_at"])

    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("routing_key", sa.String(100), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("status", outbox_status, nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'published')", name="ck_outbox_outbox_status"),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name="fk_outbox_payment_id_payments",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
    )
    op.create_index("ix_outbox_payment_id", "outbox", ["payment_id"])
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["status", "available_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_index("ix_outbox_payment_id", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_created_at", table_name="payments")
    op.drop_index("ix_payments_status", table_name="payments")
    op.drop_table("payments")

    bind = op.get_bind()
    for t in (outbox_status, webhook_status, payment_status, currency):
        t.drop(bind, checkfirst=True)
