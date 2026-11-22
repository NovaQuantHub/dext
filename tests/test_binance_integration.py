"""Test Binance WS integration with unified architecture."""
import asyncio
import hashlib
import hmac
import json
from decimal import Decimal

import pytest

from exchanges.ws.dispatcher import create_dispatcher
from exchanges.ws.binance import BinanceWS, BinanceUserWS
from exchanges.ws.types import EventType, OrderStatus, OrderSide
from conftest import MockWebsocket


@pytest.mark.asyncio
async def test_binance_bbo_to_dispatcher():
    dispatcher = create_dispatcher()
    events_received = []

    async def mock_on_event(event):
        events_received.append(event)
        await dispatcher.on_raw_event(event)

    ws_client = BinanceWS(
        symbols=["BTCUSDT"],
        on_event=mock_on_event,
        include_depth=False,
    )

    mock_ws = MockWebsocket()
    await ws_client.on_connect(mock_ws)

    bookticker_msg = json.dumps({
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "65000.10",
            "B": "0.500",
            "a": "65000.20",
            "A": "0.700",
            "E": 1705834567890,
        },
    })

    await ws_client.handle_message(bookticker_msg, ts_local_ms=1705834567895)

    assert len(events_received) == 1
    raw_event = events_received[0]
    assert raw_event["exchange"] == "binance"
    assert raw_event["symbol"] == "BTCUSDT"
    assert raw_event["stream"] == "bbo"
    assert raw_event["bids"] == [[65000.10, 0.5]]
    assert raw_event["asks"] == [[65000.20, 0.7]]

    normalized = await asyncio.wait_for(dispatcher.get_market_data(), timeout=0.1)
    assert normalized.exchange == "binance"
    assert normalized.symbol == "BTC/USDT"
    assert normalized.data.bid == Decimal("65000.10")
    assert normalized.data.ask == Decimal("65000.20")
    assert normalized.data.bid_size == Decimal("0.5")
    assert normalized.data.ask_size == Decimal("0.7")
    assert normalized.metadata.event_type == EventType.BBO_UPDATE


@pytest.mark.asyncio
async def test_binance_depth_to_dispatcher():
    dispatcher = create_dispatcher()
    events_received = []

    async def mock_on_event(event):
        events_received.append(event)
        await dispatcher.on_raw_event(event)

    ws_client = BinanceWS(
        symbols=["ETHUSDT"],
        on_event=mock_on_event,
        include_depth=True,
        depth_level=5,
        depth_interval_ms=100,
    )

    mock_ws = MockWebsocket()
    await ws_client.on_connect(mock_ws)

    depth_msg = json.dumps({
        "stream": "ethusdt@depth5@100ms",
        "data": {
            "lastUpdateId": 167043010,
            "bids": [
                ["2500.10", "1.2"],
                ["2499.90", "0.8"],
            ],
            "asks": [
                ["2500.20", "0.6"],
                ["2500.30", "1.1"],
            ],
            "E": 1705834567900,
            "s": "ETHUSDT",
        },
    })

    await ws_client.handle_message(depth_msg, ts_local_ms=1705834567905)

    assert len(events_received) == 1
    raw_event = events_received[0]
    assert raw_event["exchange"] == "binance"
    assert raw_event["symbol"] == "ETHUSDT"
    assert raw_event["stream"] == "l2"
    assert raw_event["bids"][0] == [2500.10, 1.2]
    assert raw_event["asks"][0] == [2500.20, 0.6]

    normalized = await asyncio.wait_for(dispatcher.get_market_data(), timeout=0.1)
    assert normalized.exchange == "binance"
    assert normalized.symbol == "ETH/USDT"
    assert normalized.data.bid == Decimal("2500.10")
    assert normalized.data.ask == Decimal("2500.20")
    assert normalized.data.bid_size == Decimal("1.2")
    assert normalized.data.ask_size == Decimal("0.6")


@pytest.mark.asyncio
async def test_binance_user_order_update():
    dispatcher = create_dispatcher()
    events_received = []

    async def mock_on_event(event):
        events_received.append(event)
        await dispatcher.on_raw_event(event)

    ws_client = BinanceUserWS(
        api_key="test_key",
        secret="test_secret",
        on_event=mock_on_event,
    )

    class SubscriptionSocket(MockWebsocket):
        async def recv(self):
            request = json.loads(self.sent_messages[-1])
            assert request["method"] == "userDataStream.subscribe.signature"
            params = request["params"]
            assert params["apiKey"] == "test_key"
            signed = f"apiKey=test_key&timestamp={params['timestamp']}".encode()
            assert params["signature"] == hmac.new(b"test_secret", signed, hashlib.sha256).hexdigest()
            return json.dumps({"id": request["id"], "status": 200, "result": {"subscriptionId": 0}})

    mock_ws = SubscriptionSocket()
    await ws_client.subscribe(mock_ws)

    order_msg = json.dumps({
        "e": "executionReport",
        "E": 1705834567890,
        "s": "BTCUSDT",
        "c": "test_client_id",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "0.10",
        "p": "65000.00",
        "X": "PARTIALLY_FILLED",
        "x": "TRADE",
        "i": 123456,
        "l": "0.05",
        "z": "0.05",
        "L": "65000.00",
        "n": "0.0001",
        "N": "BNB",
        "T": 1705834567880,
        "t": 987654,
        "m": False,
    })

    await ws_client.handle_message(json.dumps({"subscriptionId": 1, "event": json.loads(order_msg)}), 1705834567895)
    assert events_received == []
    await ws_client.handle_message(json.dumps({"subscriptionId": 0, "event": json.loads(order_msg)}), 1705834567895)

    assert len(events_received) == 1
    raw_event = events_received[0]
    assert raw_event["exchange"] == "binance"
    assert raw_event["stream"] == "user"
    assert raw_event["event_type"] == "executionReport"
    assert raw_event["order_id"] == "123456"

    normalized = await asyncio.wait_for(dispatcher.get_order_update(), timeout=0.1)
    assert normalized.exchange == "binance"
    assert normalized.symbol == "BTC/USDT"
    assert normalized.data.status == OrderStatus.PARTIALLY_FILLED
    assert normalized.data.side == OrderSide.BUY
    assert normalized.data.amount == Decimal("0.10")
    assert normalized.data.filled == Decimal("0.05")
    assert normalized.data.fill_price == Decimal("65000.00")
    assert normalized.metadata.event_type == EventType.FILL_UPDATE

    with pytest.raises(ConnectionError, match="eventStreamTerminated"):
        await ws_client.handle_message('{"subscriptionId":0,"event":{"e":"eventStreamTerminated"}}', 1705834567896)
    await ws_client.on_disconnect()
    await ws_client.subscribe(mock_ws)
    assert json.loads(mock_ws.sent_messages[0])["id"] != json.loads(mock_ws.sent_messages[1])["id"]
