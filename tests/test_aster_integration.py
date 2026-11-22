"""Test Aster exchange integration with unified architecture."""
import asyncio
import json
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
import httpx
from eth_account import Account
from eth_account.messages import encode_typed_data

from exchanges.aster import aster
from exchanges.base.errors import (
    ExchangeError,
    InvalidOrder,
    InsufficientFunds,
    OrderNotFound,
    AuthenticationError,
    OperationFailed,
    InvalidNonce,
    ArgumentsRequired,
)
from exchanges.ws.aster import AsterBookTickerWS, AsterUserWS
from exchanges.ws.dispatcher import create_dispatcher


def test_aster_v3_signed_requests_and_legacy_routes():
    account = Account.from_key("1" * 64)
    user = "0x" + "2" * 40
    client = aster({"privateKey": account.key.hex(), "options": {"user": user}})
    balance = client.sign("fapi/v2/balance", "private")
    order = client.sign("fapi/v1/order", "private", "POST", {"symbol": "BTCUSDT", "newClientOrderId": "example+order"})
    assert urlsplit(balance["url"]).path == "/fapi/v3/balance"
    assert order["url"].endswith("/fapi/v3/order")
    assert order["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert "X-MBX-APIKEY" not in order["headers"]

    query, signature = order["body"].rsplit("&signature=", 1)
    params = parse_qs(query)
    assert params["newClientOrderId"] == ["example+order"]
    assert params["signer"] == [account.address]
    assert params["user"] == [user]
    assert int(params["nonce"][0]) > int(parse_qs(urlsplit(balance["url"]).query)["nonce"][0])
    domain = {"name": "AsterSignTransaction", "version": "1", "chainId": 1666, "verifyingContract": "0x" + "0" * 40}
    message = encode_typed_data(domain, {"Message": [{"name": "msg", "type": "string"}]}, {"msg": query})
    assert Account.recover_message(message, signature=bytes.fromhex(signature)) == account.address

    legacy = aster({"apiKey": "legacy", "secret": "secret"}).sign("fapi/v2/balance", "private")
    assert urlsplit(legacy["url"]).path == "/fapi/v2/balance"
    assert legacy["headers"]["X-MBX-APIKEY"] == "legacy"


def test_aster_v3_listen_key_is_created_and_renewed(monkeypatch):
    async def on_event(event):
        pass

    class Response:
        content = b"{}"

        def raise_for_status(self):
            pass

        def json(self):
            return {"listenKey": "test_listen_key"}

    client = AsterUserWS(private_key="1" * 64, on_event=on_event)
    calls = []

    def request(method, url, *, data, headers, timeout):
        assert url.endswith("/fapi/v3/listenKey")
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        params = parse_qs(data)
        assert params["signature"] and params["signer"]
        assert "listenKey" not in params
        calls.append((method, int(params["nonce"][0])))
        return Response()

    monkeypatch.setattr(client.session, "request", request)
    key = client._create_listen_key()
    client._keepalive_listen_key(key)
    assert key == "test_listen_key"
    assert [method for method, _ in calls] == ["POST", "PUT"]
    assert calls[1][1] > calls[0][1]


@pytest.mark.asyncio
async def test_aster_v3_rest_order_parameters_and_balance_match_sync_and_async(monkeypatch):
    client = aster({"privateKey": "1" * 64, "enableRateLimit": False})
    exchange_info = {"symbols": [{
        "symbol": "DEMOUSDT", "baseAsset": "DEMO", "quoteAsset": "USDT", "status": "TRADING",
        "pricePrecision": 2, "quantityPrecision": 0,
        "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.25"},
                    {"filterType": "LOT_SIZE", "stepSize": "5"}],
    }]}
    balance = [{"asset": "USDT", "balance": "20", "availableBalance": "13"}]
    signed_orders = []

    def fetch(url, method, headers, body):
        path = urlsplit(url).path
        if path == "/fapi/v3/exchangeInfo":
            return exchange_info
        if path == "/fapi/v3/balance":
            assert parse_qs(urlsplit(url).query)["signature"]
            return balance
        if path == "/fapi/v3/positionRisk":
            assert parse_qs(urlsplit(url).query)["signature"]
            return [{"symbol": "DEMOUSDT", "positionAmt": "-5", "entryPrice": "100", "marginType": "cross"}]
        assert (method, path) == ("POST", "/fapi/v3/order")
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        fields = {k: v[0] for k, v in parse_qs(body).items() if k not in ("nonce", "signature", "signer")}
        signed_orders.append(fields)
        return {"orderId": 101, "symbol": "DEMOUSDT", "status": "NEW"}

    def respond(request):
        return httpx.Response(200, json=fetch(str(request.url), request.method, request.headers, request.content.decode()))

    monkeypatch.setattr(client, "fetch", fetch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as transport:
        client._async_client = transport
        assert await client.fetch_balance_async() == client.fetch_balance()
        assert not client.markets
        assert (await client.create_order_async("DEMO/USDT", "limit", "sell", 12, 101.37, {"reduceOnly": True}))["id"] == "101"
        assert client.create_order("DEMO/USDT", "stop_market", "sell", None, params={"stopPrice": "100", "closePosition": True})["id"] == "101"
        positions = await client.fetch_positions_async(["DEMO/USDT"])
        assert positions == client.fetch_positions(["DEMO/USDT"])
        assert [(position["symbol"], position["side"], position["contracts"]) for position in positions] == [("DEMO/USDT", "short", 5)]

    assert signed_orders == [
        {"symbol": "DEMOUSDT", "type": "LIMIT", "side": "SELL", "price": "101.25", "quantity": "10",
         "timeInForce": "GTC", "reduceOnly": "true"},
        {"symbol": "DEMOUSDT", "type": "STOP_MARKET", "side": "SELL", "stopPrice": "100", "closePosition": "true"},
    ]


@pytest.mark.asyncio
async def test_aster_book_ticker_reaches_market_data_queue():
    dispatcher = create_dispatcher()
    client = AsterBookTickerWS(["BTCUSDT"], dispatcher.on_raw_event)
    frame = json.dumps({
        "stream": "btcusdt@bookTicker",
        "data": {
            "e": "bookTicker",
            "s": "BTCUSDT",
            "E": 1700000000000,
            "b": "60000",
            "B": "2",
            "a": "60001",
            "A": "3",
        },
    })

    await client.handle_message(frame, ts_local_ms=1700000000001)
    update = await asyncio.wait_for(dispatcher.get_market_data(), timeout=0.1)
    assert update.symbol == "BTC/USDT"
    assert update.data.bid == Decimal("60000")
    assert update.data.ask == Decimal("60001")
    assert update.metadata.ts_exchange == 1700000000000


class TestAsterClient:
    """Test Aster REST API client."""

    @pytest.fixture
    def client(self):
        """Create Aster client without credentials."""
        return aster({})

    def test_client_initialization(self, client):
        """Test client initializes with correct settings."""
        assert client.id == "aster"
        assert client.has["fetchTicker"]
        assert client.has["createOrder"]
        assert client.has["fetchBalance"]

    def test_describe_method(self, client):
        """Test describe method returns correct metadata."""
        desc = client.describe()
        assert desc["id"] == "aster"
        assert desc["name"] == "Aster Futures"
        assert "api" in desc["urls"]

    def test_parse_error_invalid_order(self, client):
        """Test parse_error handles invalid order errors."""
        response = {"code": -1102, "msg": "Mandatory parameter 'timeInForce' was not sent"}

        with pytest.raises(ArgumentsRequired):
            client.parse_error(response)

    def test_parse_error_insufficient_funds(self, client):
        """Test parse_error handles insufficient funds errors."""
        response = {"code": -2019, "msg": "Margin is insufficient"}

        with pytest.raises(InsufficientFunds):
            client.parse_error(response)

    def test_parse_error_order_not_found(self, client):
        """Test parse_error handles order not found errors."""
        response = {"code": -2011, "msg": "Unknown order sent"}

        with pytest.raises(OrderNotFound):
            client.parse_error(response)

    def test_parse_error_authentication(self, client):
        """Test parse_error handles authentication errors."""
        response = {"code": -1022, "msg": "Signature verification failed"}

        with pytest.raises(AuthenticationError):
            client.parse_error(response)

    def test_parse_error_no_error(self, client):
        """Test parse_error does nothing for valid responses."""
        response = {"symbol": "XAUUSDT", "price": "4596.50"}

        # Should not raise
        client.parse_error(response)

    def test_parse_error_none_response(self, client):
        """Test parse_error handles None response."""
        # Should not raise
        client.parse_error(None)


class TestAsterErrorMapping:
    """Test comprehensive error code mapping."""

    @pytest.fixture
    def client(self):
        return aster({})

    @pytest.mark.parametrize("error_code,error_msg,expected_exception", [
        ("-1000", "Unknown error", OperationFailed),
        ("-1021", "Timestamp out of recv window", InvalidNonce),
        ("-1102", "Missing parameter", ArgumentsRequired),
        ("-2010", "Account insufficient balance", InvalidOrder),
        ("-2011", "Unknown order", OrderNotFound),
        ("-1022", "Invalid signature", AuthenticationError),
    ])
    def test_error_code_mapping(self, client, error_code, error_msg, expected_exception):
        """Test that error codes map to correct exception types."""
        response = {"code": int(error_code), "msg": error_msg}

        with pytest.raises(expected_exception):
            client.parse_error(response)


class TestAsterEndpoints:
    """Test Aster endpoint configuration."""

    @pytest.fixture
    def client(self):
        return aster({})

    def test_endpoints_exist(self, client):
        """Test that endpoints are properly configured."""
        assert hasattr(client, "endpoints")
        assert client.endpoints is not None

    def test_has_required_endpoints(self, client):
        """Test that required API endpoints are defined."""
        # Check public endpoints
        assert hasattr(client.endpoints, "public_get_fapi_v1_exchangeinfo")
        assert hasattr(client.endpoints, "public_get_fapi_v1_ticker_price")
        assert hasattr(client.endpoints, "public_get_fapi_v1_depth")

        # Check private endpoints
        assert hasattr(client.endpoints, "private_post_fapi_v1_order")
        assert hasattr(client.endpoints, "private_get_fapi_v2_balance")
        assert hasattr(client.endpoints, "private_delete_fapi_v1_order")


@pytest.mark.skip(reason="Requires live API credentials")
class TestAsterLiveAPI:
    """Live API tests (requires credentials)."""

    @pytest.fixture
    def client(self):
        """Create authenticated Aster client."""
        # In real tests, load from environment
        return aster({
            "apiKey": "your_api_key",
            "secret": "your_secret"
        })

    async def test_fetch_ticker(self, client):
        """Test fetching ticker data."""
        ticker = await client.fetch_ticker("XAU/USDT")
        assert "bid" in ticker
        assert "ask" in ticker
        assert ticker["symbol"] == "XAU/USDT"

    async def test_create_limit_order(self, client):
        """Test creating limit order."""
        order = await client.create_order(
            symbol="XAU/USDT",
            order_type="limit",
            side="buy",
            amount=Decimal("0.001"),
            price=Decimal("4500.0")
        )
        assert order["id"] is not None
        assert order["status"] in ["open", "NEW"]
