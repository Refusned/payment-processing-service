from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.payments import request_fingerprint
from app.schemas import MAX_METADATA_BYTES, PaymentCreate


def body(**overrides):
    data = {
        "amount": "199.99",
        "currency": "RUB",
        "description": "order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/hooks/payments",
    }
    data.update(overrides)
    return data


def test_valid_body():
    payment = PaymentCreate.model_validate(body())
    assert payment.amount == Decimal("199.99")
    assert payment.metadata == {"order_id": 42}


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://example.com/hook"},
        {"webhook_url": "not a url"},
        {"webhook_url": "https://example.com/" + "a" * 2048},
        {"description": "x" * 1025},
        {"metadata": ["not", "an", "object"]},
        {"metadata": {"blob": "x" * MAX_METADATA_BYTES}},
        {"unexpected": "field"},
    ],
)
def test_invalid_body(overrides):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(**overrides))


@pytest.mark.parametrize("missing", ["amount", "currency", "webhook_url"])
def test_required_fields(missing):
    data = body()
    del data[missing]
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(data)


def test_optional_fields_default():
    data = body()
    del data["description"], data["metadata"]
    payment = PaymentCreate.model_validate(data)
    assert payment.description is None
    assert payment.metadata == {}


def test_null_metadata_becomes_empty_object():
    assert PaymentCreate.model_validate(body(metadata=None)).metadata == {}


@pytest.mark.parametrize("amount", ["100", "100.0", "100.00", 100, 100.0])
def test_fingerprint_ignores_amount_formatting(amount):
    reference = request_fingerprint(PaymentCreate.model_validate(body(amount="100.00")))
    assert request_fingerprint(PaymentCreate.model_validate(body(amount=amount))) == reference


def test_fingerprint_ignores_metadata_key_order():
    first = PaymentCreate.model_validate(body(metadata={"a": 1, "b": 2}))
    second = PaymentCreate.model_validate(body(metadata={"b": 2, "a": 1}))
    assert request_fingerprint(first) == request_fingerprint(second)


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "100.01"},
        {"currency": "USD"},
        {"description": "other"},
        {"metadata": {"order_id": 43}},
        {"webhook_url": "https://example.com/other"},
    ],
)
def test_fingerprint_detects_changes(overrides):
    original = request_fingerprint(PaymentCreate.model_validate(body()))
    assert request_fingerprint(PaymentCreate.model_validate(body(**overrides))) != original
