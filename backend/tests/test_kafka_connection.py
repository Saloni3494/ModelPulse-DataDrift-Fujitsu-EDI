
import asyncio
from aiokafka import AIOKafkaProducer


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092"
    )

    try:
        await producer.start()
        print("Kafka connection successful!")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
