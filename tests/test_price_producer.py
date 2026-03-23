import pytest
from unittest.mock import patch, MagicMock
from producers.price_producer import fetch_and_produce


def test_price_message_structure():
    mock_producer = MagicMock()
    with patch("producers.price_producer.yf.Ticker") as mock_yf:
        mock_info = MagicMock()
        mock_info.last_price = 150.0
        mock_info.three_month_average_volume = 1000000
        mock_yf.return_value.fast_info = mock_info
        fetch_and_produce(mock_producer)

    assert mock_producer.send.called
    call_kwargs = mock_producer.send.call_args[1]
    message = call_kwargs["value"]
    assert "ticker" in message
    assert "price" in message
    assert "event_time" in message
    assert "source" in message
    assert message["source"] == "yahoo_finance"


def test_price_is_rounded():
    mock_producer = MagicMock()
    with patch("producers.price_producer.yf.Ticker") as mock_yf:
        mock_info = MagicMock()
        mock_info.last_price = 150.123456789
        mock_info.three_month_average_volume = 1000000
        mock_yf.return_value.fast_info = mock_info
        fetch_and_produce(mock_producer)

    message = mock_producer.send.call_args[1]["value"]
    assert message["price"] == round(150.123456789, 4)


def test_no_price_skips_send():
    mock_producer = MagicMock()
    with patch("producers.price_producer.yf.Ticker") as mock_yf:
        mock_info = MagicMock()
        mock_info.last_price = None
        mock_yf.return_value.fast_info = mock_info
        fetch_and_produce(mock_producer)

    mock_producer.send.assert_not_called()


def test_producer_flushes_after_batch():
    mock_producer = MagicMock()
    with patch("producers.price_producer.yf.Ticker") as mock_yf:
        mock_info = MagicMock()
        mock_info.last_price = 100.0
        mock_info.three_month_average_volume = 500000
        mock_yf.return_value.fast_info = mock_info
        fetch_and_produce(mock_producer)

    mock_producer.flush.assert_called_once()
