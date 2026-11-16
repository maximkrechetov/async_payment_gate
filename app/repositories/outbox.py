from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Outbox, OutboxStatus


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: Outbox) -> None:
        self._session.add(event)

    async def lock_pending(self, limit: int) -> Sequence[Outbox]:
        stmt = (
            select(Outbox)
            .where(Outbox.status == OutboxStatus.PENDING, Outbox.available_at <= func.now())
            .order_by(Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()
