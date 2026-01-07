import json
from confluent_kafka import Consumer, KafkaError
from src.config import settings


class KafkaMessageConsumer:
    def __init__(self, consumer_impl=None):
        if consumer_impl:
            self.consumer = consumer_impl
        else:
            conf = {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": settings.KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
            }
            self.consumer = Consumer(conf)

        self.consumer.subscribe([settings.KAFKA_TOPIC])

    def consume_one(self, timeout=1.0):
        """Polls for a single message and returns it as a dict."""
        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(
                    f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}"
                )
            else:
                print(f"Consumer error: {msg.error()}")
            return None

        try:
            data = json.loads(msg.value().decode("utf-8"))
            print(f"Received message: {data}")
            return data
        except Exception as e:
            print(f"Error decoding message: {e}")
            return None

    def consume_batch(self, batch_size=10, timeout=1.0):
        """Polls for a batch of messages and processes them."""
        msgs = self.consumer.consume(num_messages=batch_size, timeout=timeout)

        processed_messages = []
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                processed_messages.append(data)
            except Exception as e:
                print(f"Error decoding message: {e}")

        if processed_messages:
            # Print a summary of the batch for tracking
            print(f"[{settings.KAFKA_GROUP_ID}] Processed batch of {len(processed_messages)} messages.")
            if len(processed_messages) > 0:
                print(f"  Sample: {processed_messages[0]}")

        return processed_messages

    def start_consuming(self):
        """Continuous loop to consume messages efficiently using batches."""
        print(f"Starting consumer for topic: {settings.KAFKA_TOPIC}")
        print(f"Connecting to: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        try:
            while True:
                # print("DEBUG: Polling...")
                self.consume_batch(batch_size=10, timeout=1.0)
        except KeyboardInterrupt:
            print("Consumer stopped by user.")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            print("Closing consumer connection.")
            self.consumer.close()


if __name__ == "__main__":
    consumer = KafkaMessageConsumer()
    consumer.start_consuming()
