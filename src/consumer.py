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
                "fetch.min.bytes": 1024 * 16, # Wait for 16KB of data
                "fetch.wait.max.ms": 500,     # Or wait 500ms
            }
            self.consumer = Consumer(conf)

        self.consumer.subscribe([settings.KAFKA_TOPIC])

    def consume_batch(self, batch_size=100, timeout=1.0):
        """Polls for a batch of messages and processes them."""
        msgs = self.consumer.consume(num_messages=batch_size, timeout=timeout)
        if not msgs:
            return []

        processed_messages = []
        partitions_seen = set()
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                processed_messages.append(json.loads(msg.value().decode("utf-8")))
                partitions_seen.add(msg.partition())
            except Exception as e:
                print(f"Error decoding message: {e}")

        if processed_messages:
            p_ids = ", ".join(map(str, sorted(partitions_seen)))
            print(f"[{settings.KAFKA_GROUP_ID}] Processed {len(processed_messages)} msgs from partition(s): {p_ids}")
            print(f"  Sample: {processed_messages[0]}")

        return processed_messages

    def start_consuming(self):
        """Continuous loop to consume messages efficiently using batches."""
        print(f"Starting consumer for topic: {settings.KAFKA_TOPIC}")
        print(f"Connecting to: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        try:
            while True:
                # print("DEBUG: Polling...")
                self.consume_batch(batch_size=100, timeout=1.0)
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
