import json
from urllib.parse import parse_qs

from lighter.signer_client import create_api_key
from requests import Request, Response, Timeout

from exchanges.lighter_signer import SimpleSignerClient


def _make_client(monkeypatch):
    """Create a SimpleSignerClient with native library calls stubbed out."""
    monkeypatch.setattr(SimpleSignerClient, "_load_library", lambda self, d: None)
    monkeypatch.setattr(SimpleSignerClient, "_create_client", lambda self: None)
    return SimpleSignerClient(
        base_url="https://example.invalid",
        private_key="1" * 64,
        account_index=0,
        api_key_index=0,
    )


def test_place_market_order_uses_ioc_limit_and_valid_expiry(monkeypatch):
    client = _make_client(monkeypatch)

    monkeypatch.setattr(client, "fetch_market_multipliers", lambda _market_index: (10**3, 10**3))

    captured = {}

    def fake_create_order(**kwargs):
        captured.update(kwargs)
        return {"client_order_index": kwargs["client_order_index"]}, {"tx_hash": "0x0"}, None

    monkeypatch.setattr(client, "create_order", fake_create_order)

    order = client.place_market_order(market_index=0, side="sell", amount=0.05, price=3336.99)
    assert order
    assert captured["time_in_force"] == client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    assert captured["order_type"] == client.ORDER_TYPE_LIMIT
    assert captured["order_expiry"] == client.DEFAULT_IOC_EXPIRY


def test_place_market_order_uses_payload_market_index(monkeypatch):
    client = _make_client(monkeypatch)
    monkeypatch.setattr(client, "fetch_market_multipliers", lambda _market_index: (10**3, 10**3))

    def fake_create_order(**kwargs):
        return {"ClientOrderIndex": kwargs["client_order_index"], "MarketIndex": 7}, {"tx_hash": "0x0"}, None

    monkeypatch.setattr(client, "create_order", fake_create_order)
    order = client.place_market_order(market_index=999, side="buy", amount=0.01, price=1.23)
    assert order["order_id"] == str(order["raw"]["payload"]["ClientOrderIndex"])


def test_place_market_order_sets_order_id_from_uppercase(monkeypatch):
    client = _make_client(monkeypatch)
    monkeypatch.setattr(client, "fetch_market_multipliers", lambda _market_index: (10**3, 10**3))

    def fake_create_order(**kwargs):
        return {"ClientOrderIndex": kwargs["client_order_index"]}, {"tx_hash": "0x0"}, None

    monkeypatch.setattr(client, "create_order", fake_create_order)

    order = client.place_market_order(market_index=0, side="buy", amount=0.05, price=200.0)
    assert order["order_id"] == str(order["raw"]["payload"]["ClientOrderIndex"])


def test_fetch_market_multipliers_supports_spot(monkeypatch):
    client = _make_client(monkeypatch)

    class DummyResponse:
        def json(self):
            return {
                "spot_order_book_details": [
                    {"market_id": 2048, "supported_size_decimals": 4, "supported_price_decimals": 2}
                ]
            }

    class DummySession:
        def get(self, *args, **kwargs):
            return DummyResponse()

    client.session = DummySession()

    multipliers = client.fetch_market_multipliers(2048)
    assert multipliers == (10 ** 4, 10 ** 2)
    assert client._market_multipliers[2048] == multipliers


def test_current_native_signer_signs_orders_without_submitting(monkeypatch):
    private_key, _, error = create_api_key()
    assert private_key and error is None
    signer = SimpleSignerClient("https://example.invalid", private_key, account_index=10, api_key_index=3, chain_id=304)
    token, error = signer.create_auth_token_with_expiry()
    assert token and error is None

    nonces = iter(range(1, 5))
    submitted = []
    monkeypatch.setattr(signer, "_next_nonce", lambda: next(nonces))

    def post(url, *, data, timeout, verify):
        request = Request("POST", url, data=data).prepare()
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        form = parse_qs(request.body)
        if url.endswith("/sendTxBatch"):
            assert form.keys() == {"tx_types", "tx_infos"}
            tx_types = json.loads(form["tx_types"][0])
            tx_infos = json.loads(form["tx_infos"][0])
            submitted.extend((tx_type, json.loads(tx_info)) for tx_type, tx_info in zip(tx_types, tx_infos))
        else:
            assert url.endswith("/sendTx")
            assert form["price_protection"] == ["true"]
            submitted.append((int(form["tx_type"][0]), json.loads(form["tx_info"][0])))
        return _response(200, {"code": 200})

    monkeypatch.setattr(signer.session, "post", post)
    order = {"market_index": 1, "client_order_index": 10, "base_amount": 100, "price": 600000,
             "is_ask": False, "order_type": 0, "time_in_force": 1}
    assert signer.create_order(**order)[2] is None
    assert signer.cancel_order(market_index=1, order_index=10)[2] is None
    assert signer.create_order_batch([dict(order, client_order_index=11, symbol="BTC"),
                                      dict(order, client_order_index=12, symbol="BTC")])[2] is None
    assert [type_id for type_id, _ in submitted] == [14, 15, 14, 14]
    assert [payload["Nonce"] for _, payload in submitted] == [1, 2, 3, 4]


def _response(status, payload):
    response = Response()
    response.status_code = status
    response._content = json.dumps(payload).encode()
    return response


def test_lighter_refreshes_nonce_after_failure_without_replaying_unknown_transactions(monkeypatch):
    signer = _make_client(monkeypatch)
    server_nonces = iter([1, 1, 4, 9])
    submitted = []

    def get(url, *, params, timeout, verify):
        assert url.endswith("/nextNonce")
        return _response(200, {"nonce": next(server_nonces)})

    def post(url, *, data, timeout, verify):
        nonce = json.loads(data["tx_info"])["Nonce"]
        submitted.append(nonce)
        if nonce == 1:
            return _response(400, {"code": 1, "message": "invalid nonce"})
        if nonce == 5:
            raise Timeout()
        return _response(200, {"code": 200})

    monkeypatch.setattr(signer.session, "get", get)
    monkeypatch.setattr(signer.session, "post", post)

    def sign(nonce, **params):
        if params["base_amount"] == 0:
            return 0, None, "invalid size"
        return 14, json.dumps({"Nonce": nonce}), None

    monkeypatch.setattr(signer, "_sign_create_order", sign)
    order = {"market_index": 1, "client_order_index": 12, "base_amount": 1, "price": 600000,
             "is_ask": False, "order_type": 0, "time_in_force": 1}
    assert signer.create_order(**dict(order, base_amount=0))[2] == "invalid size"
    assert submitted == []
    assert signer.create_order(**order)[2] is None
    assert submitted == [1, 4]
    assert "outcome is unknown" in signer.create_order(**order)[2]
    assert submitted == [1, 4, 5]
    assert signer.create_order(**order)[2] is None
    assert submitted == [1, 4, 5, 9]
