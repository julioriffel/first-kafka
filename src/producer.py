import json
from confluent_kafka import Producer
from src.config import settings


class KafkaMessageProducer:
    def __init__(self, producer_impl=None):
        if producer_impl:
            self.producer = producer_impl
        else:
            conf = {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "linger.ms": 20,  # Delay to batch messages locally
                "batch.size": 32768,  # 32KB batch size
                "compression.type": "snappy",  # Faster than gzip, better than none
                "acks": 1,  # Speed over durability (adjustable)
            }
            self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        """Called once for each message transmitted to admit success or failure."""
        if err is not None:
            # Throttle failed logs to avoid flooding terminal
            print(f"Message delivery failed: {err}")
        # else:
        # Removed success logs for high throughput to avoid I/O blocking

    def send_message(self, topic, data, key=None, poll=True):
        """Sends a JSON message to a Kafka topic asynchronously."""
        message_bytes = json.dumps(data).encode("utf-8")
        key_bytes = str(key).encode("utf-8") if key else None
        self.producer.produce(
            topic, message_bytes, key=key_bytes, callback=self.delivery_report
        )
        # Periodically poll for delivery reports to keep the outgoing queue in check
        if poll:
            self.producer.poll(0)

    def close(self):
        """Flush outstanding messages and close the producer."""
        print("Flushing outstanding messages...")
        self.producer.flush()


def main(count=50000):
    try:
        producer = KafkaMessageProducer()
        print(f"Starting performance test: Sending {count} messages...")

        for a in range(count):
            test_message = {"hello": "world", "status": f"prod_perf_{a}"}
            # Only poll every 100 messages to process callbacks, reducing CPU overhead
            do_poll = a % 100 == 0
            producer.send_message(
                settings.KAFKA_TOPIC, test_message, key=a, poll=do_poll
            )

        print("Performance test: Messages sent to buffer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
