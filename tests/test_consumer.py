from unittest.mock import MagicMock
from src.consumer import KafkaMessageConsumer


def test_consumer_processes_message():
    # Arrange
    mock_kafka_consumer = MagicMock()
    # Mock return value for poll
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"status": "ok"}'
    mock_msg.topic.return_value = "test-topic"
    mock_msg.partition.return_value = 0

    # Simulate a single poll returning a message, then None to break loop (if implemented with break)
    mock_kafka_consumer.poll.side_effect = [mock_msg, None]

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_consumer)

    # Act
    # We'll call a method that processes one message to keep it simple for unit test
    received = consumer.consume_one()

    # Assert
    assert received == {"status": "ok"}
    mock_kafka_consumer.subscribe.assert_called_once()
