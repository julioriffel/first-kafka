from unittest.mock import MagicMock, patch
from src.consumer import KafkaMessageConsumer
from confluent_kafka import KafkaError


def test_consumer_initialization():
    mock_kafka_impl = MagicMock()
    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    assert consumer.consumer == mock_kafka_impl
    mock_kafka_impl.subscribe.assert_called_once()


@patch("src.consumer.Consumer")
def test_consumer_default_initialization(mock_consumer_cls):
    """Test initialization without passing a consumer implementation."""
    consumer = KafkaMessageConsumer()
    mock_consumer_cls.assert_called_once()
    assert consumer.consumer == mock_consumer_cls.return_value
    consumer.consumer.subscribe.assert_called_once()


def test_consume_batch_success():
    mock_kafka_impl = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"hello": "world"}'
    mock_msg.partition.return_value = 5

    # Return one message
    mock_kafka_impl.consume.return_value = [mock_msg]

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch(batch_size=1, timeout=0.1)

    assert len(messages) == 1
    assert messages[0] == {"hello": "world"}


def test_consume_batch_empty():
    mock_kafka_impl = MagicMock()
    mock_kafka_impl.consume.return_value = []

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch()

    assert messages == []


def test_consume_batch_with_error():
    mock_kafka_impl = MagicMock()
    mock_msg_error = MagicMock()
    mock_err = MagicMock()
    mock_err.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART  # Some error
    mock_msg_error.error.return_value = mock_err

    mock_msg_success = MagicMock()
    mock_msg_success.error.return_value = None
    mock_msg_success.value.return_value = b'{"valid": "data"}'
    mock_msg_success.partition.return_value = 0

    mock_kafka_impl.consume.return_value = [mock_msg_error, mock_msg_success]

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch()

    assert len(messages) == 1
    assert messages[0] == {"valid": "data"}


def test_consume_batch_partition_eof():
    mock_kafka_impl = MagicMock()
    mock_msg_eof = MagicMock()
    mock_err = MagicMock()
    mock_err.code.return_value = KafkaError._PARTITION_EOF
    mock_msg_eof.error.return_value = mock_err

    mock_kafka_impl.consume.return_value = [mock_msg_eof]

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch()

    assert messages == []


def test_consume_batch_decode_error():
    mock_kafka_impl = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b"invalid json"

    mock_kafka_impl.consume.return_value = [mock_msg]

    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch()

    assert messages == []


def test_start_consuming_loop():
    """Test start_consuming by mocking consume_batch to raise KeyboardInterrupt immediately."""
    mock_kafka_impl = MagicMock()
    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)

    # Side effect: first call raises KeyboardInterrupt
    consumer.consume_batch = MagicMock(side_effect=KeyboardInterrupt)

    consumer.start_consuming()

    consumer.consume_batch.assert_called_once()
    mock_kafka_impl.close.assert_called_once()


def test_start_consuming_exception():
    """Test start_consuming handling generic exceptions."""
    mock_kafka_impl = MagicMock()
    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)

    # Side effects: First raises an error, second raises KeyboardInterrupt to exit loop
    consumer.consume_batch = MagicMock(
        side_effect=[Exception("Boom"), KeyboardInterrupt]
    )

    consumer.start_consuming()

    assert consumer.consume_batch.call_count == 1
    mock_kafka_impl.close.assert_called_once()
