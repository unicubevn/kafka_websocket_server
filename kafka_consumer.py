from confluent_kafka import Consumer, KafkaException
import asyncio

class KafkaConsumerManager:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}  # topic -> asyncio task

    def create_consumer(self, topic, websocket_handler):
        if topic not in self.consumers:
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f'group_{topic}',
                'auto.offset.reset': 'earliest'
            })

            consumer.subscribe([topic])

            async def consume():
                try:
                    while True:
                        message = consumer.poll(timeout=1.0)
                        if message is None:
                            continue
                        if message.error():
                            raise KafkaException(message.error())
                        await websocket_handler.broadcast_message(topic, message.value().decode('utf-8'))
                except KafkaException as e:
                    print(f"Error consuming from topic {topic}: {str(e)}")
                finally:
                    consumer.close()

            # Run the consumer in an asyncio task
            self.consumers[topic] = asyncio.create_task(consume())

    def stop(self):
        for task in self.consumers.values():
            task.cancel()
