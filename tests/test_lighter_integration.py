"""Test Lighter WS integration with unified architecture."""
import asyncio
import json
from decimal import Decimal

import pytest

from exchanges.base.errors import InvalidOrder
from exchanges.lighter import lighter
from exchanges.ws.dispatcher import create_dispatcher
from exchanges.ws.lighter import LighterWS
from exchanges.ws.types import EventType, OrderStatus
from conftest import MockWebsocket


def test_lighter_public_rest_response(monkeypatch):
    client = lighter()
    market = {"symbol": "BTC", "market_id": 1, "status": "active", "last_trade_price": 60000.5}

    def request(endpoint, params):
        if endpoint is client.endpoints.publicGetOrderbooks:
            return {"order_book_details": [market, {"symbol": "ETH", "market_id": 0, "status": "inactive"}]}
        if endpoint is client.endpoints.publicGetOrderbooksMarketIdDetails:
            assert params == {"market_id": "1"}
            return {"order_book_details": [market]}
        assert endpoint is client.endpoints.publicGetOrderbooksMarketId
        assert params == {"market_id": "1", "limit": 100}
        return {
            "bids": [{"price": "60000.0", "initial_base_amount": "1.0", "remaining_base_amount": "0.25"}],
            "asks": [["60001.0", "0.5"]],
        }

    monkeypatch.setattr(client, "_request", request)
    markets = client.load_markets()
    assert markets["BTC"]["active"] is True
    assert markets["ETH"]["active"] is False
    assert client.fetch_ticker("BTC")["last"] == 60000.5
    assert client.fetch_tickers(["BTC"])["BTC"]["last"] == 60000.5
    book = client.fetch_order_book("BTC")
    assert book["bids"] == [[60000.0, 0.25]]
    assert book["asks"] == [[60001.0, 0.5]]


def test_lighter_candles_recent_trades_and_short_position(monkeypatch):
    client = lighter()
    monkeypatch.setattr(client, "load_markets", lambda: None)
    monkeypatch.setattr(client, "market", lambda symbol: {"id": "1", "symbol": symbol})

    def request(endpoint, params):
        if endpoint.path == "api/v1/candles":
            return {"c": [{"t": 1700000000000, "o": 60500, "h": 61200, "l": 60000, "c": 61000, "v": 3}]}
        assert endpoint.path == "api/v1/recentTrades"
        assert params == {"market_id": "1", "limit": 100}
        return {"trades": [{"trade_id_str": "12", "price": "61000", "size": "1", "timestamp": 1700000000000,
                            "is_maker_ask": False}]}

    monkeypatch.setattr(client, "_request", request)
    assert client.fetch_ohlcv("BTC") == [[1700000000000, 60500, 61200, 60000, 61000, 3]]
    trade, = client.fetch_trades("BTC")
    assert trade["side"] == "sell"
    assert trade["timestamp"] == 1700000000000
    assert trade["id"] == "12"
    position = client.parse_position({"symbol": "BTC", "market_id": 1, "position": "2", "sign": -1,
                                      "avg_entry_price": "61100", "liquidation_price": "75000"})
    assert position["side"] == "short"
    assert position["contracts"] == 2
    assert position["entryPrice"] == 61100
    assert position["liquidationPrice"] == 75000


def test_lighter_fetch_order_by_client_index_after_fill(monkeypatch):
    client = lighter({"options": {"accountIndex": 5}})
    monkeypatch.setattr(client, "fetch_open_orders", lambda **kwargs: [])
    monkeypatch.setattr(client, "market", lambda symbol: {"id": "1", "symbol": symbol})

    def request(endpoint, params):
        assert endpoint.path == "api/v1/accountOrders"
        assert params == {"account_index": 5, "client_order_indexes": "123"}
        return {"orders": [{
            "market_index": 1, "order_id": "1000", "client_order_index": 123,
            "status": "filled", "price": "61000", "initial_base_amount": "1",
            "filled_base_amount": "1", "remaining_base_amount": "0", "is_ask": False,
        }]}

    monkeypatch.setattr(client, "_request", request)
    order = client.fetch_order("123", "BTC")
    assert order["id"] == "1000"
    assert order["clientOrderId"] == "123"
    assert order["status"] == "closed"


def test_lighter_single_and_batch_orders_share_ioc_rules(monkeypatch):
    client = lighter()
    market = {"id": "1", "symbol": "BTC", "precision": {"amount": 0, "price": 0}}
    submitted = []

    class Signer:
        def create_order(self, **params):
            submitted.append(params)
            return {}, {"code": 200}, None

        def create_order_batch(self, orders):
            submitted.extend(orders)
            return [{} for _ in orders], {"code": 200}, None

    monkeypatch.setattr(client, "_ensure_signer_client", Signer)
    monkeypatch.setattr(client, "load_markets", lambda: None)
    monkeypatch.setattr(client, "market", lambda symbol: market)
    monkeypatch.setattr(client, "safe_order", lambda order, market: order)

    client.create_order("BTC", "limit", "sell", 3, 60000, {"timeInForce": "IOC", "reduceOnly": "false"})
    order = {"symbol": "BTC", "type": "limit", "side": "sell", "amount": 3, "price": 60000,
             "timeInForce": "IOC", "orderExpiry": 0, "reduceOnly": "false"}
    client.create_orders([order])
    for signed in submitted:
        assert signed["time_in_force"] == 0
        assert signed["order_expiry"] == 0
        assert signed["base_amount"] == 3
        assert signed["price"] == 60000
        assert signed["reduce_only"] is False

    with pytest.raises(InvalidOrder, match="FOK"):
        client.create_order("BTC", "limit", "sell", 3, 60000, {"timeInForce": "FOK"})
    with pytest.raises(InvalidOrder, match="FOK"):
        client.create_orders([order, dict(order, timeInForce="FOK")])
    assert len(submitted) == 2


@pytest.mark.asyncio
async def test_lighter_bbo_to_dispatcher():
    """Test Lighter BBO events flow through dispatcher."""
    # Setup
    dispatcher = create_dispatcher(lighter_market_mapping={0: "ETH/USDC"})

    events_received = []

    async def mock_on_event(event):
        events_received.append(event)
        # Also send to dispatcher
        await dispatcher.on_raw_event(event)

    ws_client = LighterWS(
        market_index=0,
        on_event=mock_on_event,
        bbo_only=True,
    )

    # Simulate connection
    mock_ws = MockWebsocket()
    await ws_client.on_connect(mock_ws)

    # Simulate snapshot message
    snapshot_msg = json.dumps({
        "type": "subscribed/order_book",
        "order_book": {
            "offset": 100,
            "bids": [
                {"price": "3250.50", "size": "10.5"},
                {"price": "3250.00", "size": "5.0"},
            ],
            "asks": [
                {"price": "3251.00", "size": "8.2"},
                {"price": "3251.50", "size": "3.0"},
            ],
        }
    })

    await ws_client.handle_message(snapshot_msg, ts_local_ms=1705834567891)

    # Simulate update message
    update_msg = json.dumps({
        "type": "update/order_book",
        "order_book": {
            "offset": 101,
            "timestamp": 1705834567890,
            "code": "ETH_USDC",
            "bids": [
                {"price": "3250.75", "size": "12.0"},  # New best bid
            ],
            "asks": [],
        }
    })

    await ws_client.handle_message(update_msg, ts_local_ms=1705834567892)

    # Verify raw events
    assert len(events_received) == 1  # Only update event (snapshot doesn't emit)

    raw_event = events_received[0]
    assert raw_event["exchange"] == "lighter"
    assert raw_event["stream"] == "bbo"
    assert raw_event["market_index"] == 0
    assert raw_event["best_bid"] == 3250.75
    assert raw_event["best_ask"] == 3251.00  # From snapshot

    # Verify normalized event from dispatcher
    normalized = await asyncio.wait_for(
        dispatcher.get_market_data(),
        timeout=0.1
    )

    assert normalized.exchange == "lighter"
    assert normalized.symbol == "ETH/USDC"  # Mapped by dispatcher
    assert normalized.data.bid == Decimal("3250.75")
    assert normalized.data.ask == Decimal("3251.00")
    assert normalized.data.bid_size == Decimal("12.0")
    assert normalized.data.ask_size == Decimal("8.2")
    assert normalized.metadata.event_type == EventType.BBO_UPDATE
    assert normalized.metadata.latency_ms >= 0

    # Verify spread calculation
    assert normalized.data.spread == Decimal("0.25")
    spread_bps = normalized.data.spread_bps
    assert 0 < spread_bps < 10  # Should be ~0.77 bps


@pytest.mark.asyncio
async def test_lighter_orders_to_dispatcher():
    """Test Lighter account orders flow through dispatcher."""
    dispatcher = create_dispatcher(lighter_market_mapping={0: "ETH/USDC"})

    events_received = []

    async def mock_on_event(event):
        events_received.append(event)
        await dispatcher.on_raw_event(event)

    ws_client = LighterWS(
        market_index=0,
        on_event=mock_on_event,
        account_index=0,
        lighter_client=None,  # Mock
    )

    mock_ws = MockWebsocket()
    await ws_client.on_connect(mock_ws)

    # Simulate order update
    order_msg = json.dumps({
        "type": "update/account_orders",
        "timestamp": 1705834567890,
        "orders": {
            "0": [
                {
                    "order_index": "12345",
                    "client_order_index": 100,
                    "status": "filled",
                    "is_ask": False,
                    "price": "3250.50",
                    "initial_base_amount": "10.0",
                    "filled_base_amount": "10.0",
                    "remaining_base_amount": "0.0",
                    "filled_quote_amount": "32505.0",
                }
            ]
        }
    })

    await ws_client.handle_message(order_msg, ts_local_ms=1705834567895)

    # Verify raw event
    assert len(events_received) == 1
    raw_event = events_received[0]
    assert raw_event["exchange"] == "lighter"
    assert raw_event["stream"] == "account_orders"
    assert len(raw_event["orders"]) == 1

    # Verify fill_price was calculated
    order = raw_event["orders"][0]
    assert "fill_price" in order
    assert order["fill_price"] == 3250.5  # 32505.0 / 10.0

    # Verify normalized order from dispatcher
    normalized = await asyncio.wait_for(
        dispatcher.get_order_update(),
        timeout=0.1
    )

    assert normalized.exchange == "lighter"
    assert normalized.symbol == "ETH/USDC"
    assert normalized.data.order_id == "12345"
    assert normalized.data.client_order_id == "100"
    assert normalized.data.status == OrderStatus.FILLED
    assert normalized.data.side.value == "buy"
    assert normalized.data.price == Decimal("3250.50")
    assert normalized.data.filled == Decimal("10.0")
    assert normalized.data.remaining == Decimal("0.0")
    assert normalized.data.fill_price == Decimal("3250.5")
    assert normalized.metadata.event_type == EventType.FILL_UPDATE


@pytest.mark.asyncio
async def test_lighter_sequence_gap_recovery():
    """Test sequence gap detection triggers snapshot refresh."""
    events_received = []

    async def mock_on_event(event):
        events_received.append(event)

    ws_client = LighterWS(
        market_index=0,
        on_event=mock_on_event,
        bbo_only=False,
    )

    mock_ws = MockWebsocket()
    await ws_client.on_connect(mock_ws)

    # Snapshot with offset 100
    snapshot = json.dumps({
        "type": "subscribed/order_book",
        "order_book": {
            "offset": 100,
            "bids": [{"price": "3250", "size": "10"}],
            "asks": [{"price": "3251", "size": "10"}],
        }
    })
    await ws_client.handle_message(snapshot, ts_local_ms=1000)

    # Normal update (offset 101)
    update1 = json.dumps({
        "type": "update/order_book",
        "order_book": {
            "offset": 101,
            "bids": [{"price": "3250.5", "size": "5"}],
            "asks": [],
        }
    })
    await ws_client.handle_message(update1, ts_local_ms=1001)

    assert len(events_received) == 1  # One valid update

    # Gap! Jump from 101 to 110 (gap > max_offset_skip)
    update2 = json.dumps({
        "type": "update/order_book",
        "order_book": {
            "offset": 110,
            "bids": [{"price": "3251", "size": "20"}],
            "asks": [],
        }
    })
    await ws_client.handle_message(update2, ts_local_ms=1002)

    # Should NOT emit event due to sequence gap
    assert len(events_received) == 1  # Still only 1 event

    # Verify unsubscribe + subscribe was sent
    assert len(mock_ws.sent_messages) >= 2

    # Find unsubscribe and subscribe messages
    messages = [json.loads(msg) for msg in mock_ws.sent_messages]
    unsub = next((m for m in messages if m.get("type") == "unsubscribe"), None)
    sub = next((m for m in messages if m.get("type") == "subscribe"), None)

    assert unsub is not None
    assert sub is not None
    assert "order_book/0" in unsub.get("channel", "")


def test_lighter_bbo_only_update_logic():
    """Test BBO-only mode updates correctly."""
    ws_client = LighterWS(
        market_index=0,
        on_event=lambda e: None,
        bbo_only=True,
    )

    # Initial state
    assert ws_client.best_bid is None
    assert ws_client.best_ask is None

    # Update bids
    ws_client._update_bbo_only("bids", [
        {"price": "100", "size": "10"},
        {"price": "99", "size": "20"},
        {"price": "101", "size": "5"},  # This should be best
    ])

    assert ws_client.best_bid == 101.0
    assert ws_client.best_bid_size == 5.0

    # Update asks
    ws_client._update_bbo_only("asks", [
        {"price": "102", "size": "8"},  # This should be best
        {"price": "103", "size": "12"},
    ])

    assert ws_client.best_ask == 102.0
    assert ws_client.best_ask_size == 8.0

    # Remove current best bid
    ws_client._update_bbo_only("bids", [
        {"price": "101", "size": "0"},  # Remove
    ])

    # Should clear best bid since we removed it
    assert ws_client.best_bid is None
    assert ws_client.best_bid_size is None


if __name__ == "__main__":
    # Run simple test
    asyncio.run(test_lighter_bbo_to_dispatcher())
    print("✅ All Lighter integration tests passed!")
