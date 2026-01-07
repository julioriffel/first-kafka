from unittest.mock import MagicMock, patch
from src.consumer import KafkaMessageConsumer
from confluent_kafka import KafkaError


def test_consumer_initialization():
    mock_kafka_impl = MagicMock()
    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    assert consumer.consumer == mock_kafka_impl
    mock_kafka_impl.subscribe.assert_called_once()
    assert consumer.retry_producer is not None


@patch("src.consumer.Consumer")
@patch("src.consumer.KafkaMessageProducer")
def test_consumer_default_initialization(mock_producer_cls, mock_consumer_cls):
    """Test initialization without passing a consumer implementation."""
    consumer = KafkaMessageConsumer()
    mock_consumer_cls.assert_called_once()
    mock_producer_cls.assert_called_once()
    assert consumer.consumer == mock_consumer_cls.return_value
    assert consumer.retry_producer == mock_producer_cls.return_value
    consumer.consumer.subscribe.assert_called_once()


@patch("random.randint")
def test_consume_batch_success_lucky_7(mock_randint):
    mock_randint.return_value = 7  # Force success
    mock_kafka_impl = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"hello": "world"}'
    mock_msg.partition.return_value = 5

    mock_kafka_impl.consume.return_value = [mock_msg]

    consumer = KafkaMessageConsumer(
        consumer_impl=mock_kafka_impl, producer_impl=MagicMock()
    )
    messages = consumer.consume_batch(batch_size=1, timeout=0.1)

    assert len(messages) == 1
    assert messages[0] == {"hello": "world"}
    consumer.retry_producer.send_message.assert_not_called()


@patch("random.randint")
def test_consume_batch_retry_unlucky(mock_randint):
    mock_randint.return_value = 2  # Not 7, retry
    mock_kafka_impl = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"hello": "world", "retry_count": 0}'
    mock_msg.partition.return_value = 5

    mock_kafka_impl.consume.return_value = [mock_msg]
    mock_producer = MagicMock()

    consumer = KafkaMessageConsumer(
        consumer_impl=mock_kafka_impl, producer_impl=mock_producer
    )
    messages = consumer.consume_batch(batch_size=1, timeout=0.1)

    # Logic: if not 7, we handle it by producing, but we don't return it as 'processed' (success)
    assert len(messages) == 0
    mock_producer.send_message.assert_called_once()
    call_args = mock_producer.send_message.call_args
    assert call_args[0][1]["retry_count"] == 1  # Incremented
    assert call_args[0][1]["hello"] == "world"


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

    mock_kafka_impl.consume.return_value = [mock_msg_error]
    consumer = KafkaMessageConsumer(consumer_impl=mock_kafka_impl)
    messages = consumer.consume_batch()
    # Error should be skipped
    assert len(messages) == 0


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
    mock_producer = MagicMock()
    consumer = KafkaMessageConsumer(
        consumer_impl=mock_kafka_impl, producer_impl=mock_producer
    )

    # Side effect: first call raises KeyboardInterrupt
    consumer.consume_batch = MagicMock(side_effect=KeyboardInterrupt)

    consumer.start_consuming()

    consumer.consume_batch.assert_called_once()
    mock_kafka_impl.close.assert_called_once()
    mock_producer.close.assert_called_once()


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
