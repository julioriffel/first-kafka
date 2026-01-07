import json
from confluent_kafka import Producer
from src.config import settings


class KafkaMessageProducer:
    def __init__(self, producer_impl=None):
        if producer_impl:
            self.producer = producer_impl
        else:
            conf = {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
            self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        """Called once for each message transmitted to admit success or failure."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, topic, data):
        """Sends a JSON message to a Kafka topic asynchronously."""
        message_bytes = json.dumps(data).encode("utf-8")
        self.producer.produce(topic, message_bytes, callback=self.delivery_report)
        # Periodically poll for delivery reports to keep the outgoing queue in check
        self.producer.poll(0)

    def close(self):
        """Flush outstanding messages and close the producer."""
        print("Flushing outstanding messages...")
        self.producer.flush()


if __name__ == "__main__":
    try:
        producer = KafkaMessageProducer()
        for a in range(10000):
            test_message = {"hello": "world", "status": f"producer test {a}"}
            producer.send_message(settings.KAFKA_TOPIC, test_message)
    finally:
        producer.close()
