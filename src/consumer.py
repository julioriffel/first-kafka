import json
import random
from confluent_kafka import Consumer, KafkaError
from src.config import settings
from src.producer import KafkaMessageProducer


class KafkaMessageConsumer:
    def __init__(self, consumer_impl=None, producer_impl=None):
        if consumer_impl:
            self.consumer = consumer_impl
        else:
            conf = {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": settings.KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "fetch.min.bytes": 1024 * 16,  # Wait for 16KB of data
                "fetch.wait.max.ms": 500,  # Or wait 500ms
            }
            self.consumer = Consumer(conf)

        if producer_impl:
            self.retry_producer = producer_impl
        else:
            self.retry_producer = KafkaMessageProducer()

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
                # Direct bytes-to-dict deserialization avoids intermediate string allocation (approx 30% faster)
                data = json.loads(msg.value())

                # Random retry logic
                rand_val = random.randint(0, 10)

                if rand_val == 7:
                    retry_count = data.get("retry_count", 0)
                    print(
                        f"[{settings.KAFKA_GROUP_ID}] LUCKY 7! Processed after {retry_count} retries. Msg: {data}"
                    )
                    processed_messages.append(data)
                    partitions_seen.add(msg.partition())
                else:
                    # Increment retry count and send back to queue
                    data["retry_count"] = data.get("retry_count", 0) + 1
                    # print(f"Retry {rand_val}!=7. Re-queueing. Count: {data['retry_count']}")
                    self.retry_producer.send_message(
                        settings.KAFKA_TOPIC, data, poll=False
                    )
                    # We accept that this message is 'handled' by being re-queued, so we don't treat it as an error

            except Exception as e:
                print(f"Error decoding or processing message: {e}")

        if processed_messages:
            p_ids = ", ".join(map(str, sorted(partitions_seen)))
            print(
                f"[{settings.KAFKA_GROUP_ID}] Batch Summary: {len(processed_messages)} successes from partition(s): {p_ids}"
            )

        return processed_messages

    def start_consuming(self):
        """Continuous loop to consume messages efficiently using batches."""
        print(f"Starting consumer for topic: {settings.KAFKA_TOPIC}")
        print(f"Connecting to: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        try:
            while True:
                # print("DEBUG: Polling...")
                self.consume_batch(batch_size=100, timeout=1.0)
                # Ensure the producer sends out re-queued messages efficiently
                self.retry_producer.producer.poll(0)
        except KeyboardInterrupt:
            print("Consumer stopped by user.")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            print("Closing consumer connection.")
            self.consumer.close()
            print("Flushing retry producer.")
            self.retry_producer.close()


def main():
    consumer = KafkaMessageConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()
