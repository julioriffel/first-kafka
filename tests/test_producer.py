import json
from unittest.mock import MagicMock, patch
from src.producer import KafkaMessageProducer, main


def test_producer_initialization():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)
    assert producer.producer == mock_kafka_impl


@patch("src.producer.Producer")
def test_producer_default_initialization(mock_producer_cls):
    """Test initialization without passing a producer implementation."""
    producer = KafkaMessageProducer()
    mock_producer_cls.assert_called_once()
    assert producer.producer == mock_producer_cls.return_value


def test_producer_send_message_basic():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)
    topic = "test-topic"
    data = {"foo": "bar"}

    producer.send_message(topic, data, key="key123", poll=True)

    mock_kafka_impl.produce.assert_called_once()
    args, kwargs = mock_kafka_impl.produce.call_args
    assert args[0] == topic
    assert json.loads(args[1].decode("utf-8")) == data
    assert kwargs["key"] == b"key123"
    assert kwargs["callback"] == producer.delivery_report
    mock_kafka_impl.poll.assert_called_with(0)


def test_producer_send_message_no_poll():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)

    producer.send_message("topic", {"d": 1}, poll=False)
    mock_kafka_impl.poll.assert_not_called()


def test_producer_delivery_report_error():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)
    # This just prints for now, so we just ensure it doesn't crash
    producer.delivery_report("Some error", MagicMock())


def test_producer_delivery_report_success():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)
    producer.delivery_report(None, MagicMock())


def test_producer_close():
    mock_kafka_impl = MagicMock()
    producer = KafkaMessageProducer(producer_impl=mock_kafka_impl)
    producer.close()
    mock_kafka_impl.flush.assert_called_once()


@patch("src.producer.KafkaMessageProducer")
def test_main(mock_producer_cls):
    """Test the main function runs the performance test loop."""
    mock_producer = mock_producer_cls.return_value

    # Run a small test with 2 messages
    main(count=2)

    # Should create producer
    mock_producer_cls.assert_called_once()

    # Should send messages roughly 'count' times
    assert mock_producer.send_message.call_count == 2

    # Should close
    mock_producer.close.assert_called_once()
