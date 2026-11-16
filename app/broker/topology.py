from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import settings

PAYMENTS_NEW_RK = "payments.new"
RETRY_RK = "payments.retry"
DEAD_RK = "payments.dead"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DLX = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW_RK,
    durable=True,
    routing_key=PAYMENTS_NEW_RK,
    arguments={"x-dead-letter-exchange": DLX.name, "x-dead-letter-routing-key": DEAD_RK},
)

RETRY_QUEUE = RabbitQueue(
    RETRY_RK,
    durable=True,
    routing_key=RETRY_RK,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW_RK,
    },
)

DLQ = RabbitQueue("payments.dlq", durable=True, routing_key=DEAD_RK)


async def _declare_and_bind(
    broker: RabbitBroker,
    queue: RabbitQueue,
    exchange: RabbitExchange,
    routing_key: str,
) -> None:
    declared_exchange = await broker.declare_exchange(exchange)
    declared_queue = await broker.declare_queue(queue)
    await declared_queue.bind(declared_exchange, routing_key=routing_key)


async def setup_topology(broker: RabbitBroker) -> None:
    await _declare_and_bind(broker, PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, PAYMENTS_NEW_RK)
    await _declare_and_bind(broker, RETRY_QUEUE, RETRY_EXCHANGE, RETRY_RK)
    await _declare_and_bind(broker, DLQ, DLX, DEAD_RK)


async def ensure_topology() -> None:
    async with RabbitBroker(settings.rabbitmq.url) as broker:
        await setup_topology(broker)
