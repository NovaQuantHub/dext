"""
Aster Exchange Authentication Module

Aster Futures V3 uses EIP-712, and legacy V1 uses HMAC-SHA256.
"""
import hmac
import hashlib
from threading import Lock
from time import time_ns
from typing import Dict, Any, Optional
from urllib.parse import urlencode

from eth_account import Account
from eth_account.messages import encode_typed_data

from exchanges.base.errors import AuthenticationError
from exchanges.logger import setup_logger

logger = setup_logger("exchanges.auth.aster")

_V3_DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}
_V3_MESSAGE_TYPES = {"Message": [{"name": "msg", "type": "string"}]}
_nonce_lock = Lock()
_last_nonce = 0


def sign_v3_request(params: Dict[str, Any], private_key: str, user: Optional[str] = None) -> str:
    """Return the encoded parameters and signature for an agent-wallet request."""
    if not private_key:
        raise AuthenticationError("Aster V3 requires the registered agent wallet private key")
    try:
        account = Account.from_key(private_key)
    except (TypeError, ValueError) as exc:
        raise AuthenticationError("Aster V3 agent wallet private key is invalid") from exc

    global _last_nonce
    with _nonce_lock:
        _last_nonce = nonce = max(time_ns() // 1000, _last_nonce + 1)

    signed_params = {key: value for key, value in params.items() if key not in ("signature", "user", "recvWindow", "timestamp")}
    signed_params.update(nonce=nonce, signer=account.address)
    if user:
        signed_params["user"] = user
    for key, value in signed_params.items():
        if isinstance(value, bool):
            signed_params[key] = str(value).lower()
    query = urlencode(signed_params)
    message = encode_typed_data(_V3_DOMAIN, _V3_MESSAGE_TYPES, {"msg": query})
    signature = account.sign_message(message).signature.hex()
    return f"{query}&signature={signature}"


def create_signature(secret_key: str, query_string: str) -> str:
    """
    Create HMAC-SHA256 signature for Aster API requests.

    Args:
        secret_key: API secret key
        query_string: Query string to sign

    Returns:
        Hexadecimal signature string
    """
    return hmac.new(
        secret_key.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


def sign_request(params: Dict[str, Any], secret_key: str) -> str:
    """
    Sign request parameters for Aster API.

    Args:
        params: Request parameters dictionary
        secret_key: API secret key

    Returns:
        HMAC-SHA256 signature
    """
    # Sort parameters alphabetically and create query string
    sorted_params = sorted(params.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
    return create_signature(secret_key, query_string)
