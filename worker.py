import asyncio
import aio_pika


async def process_message(message: aio_pika.IncomingMessage, queue_name: str):
    """ML inference emulation."""
    async with message.process():
        print(f"[{queue_name.upper()}] Processing: {message.body.decode()}")
        # Simulate work
        await asyncio.sleep(5)
        print("finish processing")


async def main():
    # 1. Connect to RabbitMQ
    connection = await aio_pika.connect_robust("amqp://rabbitmq:look_in_vault@localhost:5672/ml-service")

    async with connection:
        # 2. Open a channel
        channel = await connection.channel()

        # 3. Declare your queues
        high_queue = await channel.declare_queue("high_priority_queue", durable=True)
        low_queue = await channel.declare_queue("low_priority_queue", durable=True)

        print("Starting prioritized consumer loop...")

        while True:
            # Try fetching a message from the high-priority queue first
            # fail=False ensures it returns None immediately if the queue is empty
            message = await high_queue.get(fail=False)

            if message is not None:
                await process_message(message, "high")
                # Immediately skip back to the top of the loop
                # to keep draining the high priority queue
                continue

            # If high-priority is empty, check the low-priority queue
            message = await low_queue.get(fail=False)

            if message is not None:
                await process_message(message, "low")
                continue

            # If BOTH queues are completely empty, back off briefly to prevent CPU throttling
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
