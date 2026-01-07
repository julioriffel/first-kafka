import json
from unittest.mock import MagicMock
from src.producer import KafkaMessageProducer


def test_producer_sends_message():
    # Arrange
    mock_kafka_producer = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_producer)
    topic = "test-topic"
    message = {"key": "value"}

    # Act
    producer.send_message(topic, message)

    # Assert
    mock_kafka_producer.produce.assert_called_once()
    args, kwargs = mock_kafka_producer.produce.call_args
    assert args[0] == topic
    assert json.loads(args[1]) == message
