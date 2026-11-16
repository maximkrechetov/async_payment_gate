from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Response, status

from app.api.deps import PaymentServiceDep
from app.api.security import require_api_key
from app.db.models import Payment
from app.schemas.payments import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    data: PaymentCreate,
    response: Response,
    service: PaymentServiceDep,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    result = await service.create(data, idempotency_key)
    if result.replayed:
        response.headers["X-Idempotent-Replay"] = "true"
    return PaymentAccepted(
        payment_id=result.payment.id,
        status=result.payment.status,
        created_at=result.payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> Payment:
    return await service.get(payment_id)
