import random

import pytest

from app.config import Settings
from app.gateway import EmulatedGateway
from app.models import Payment


def gateway(success_rate=0.9, test_hooks=False, seed=1):
    return EmulatedGateway(
        min_delay=0, max_delay=0, success_rate=success_rate, test_hooks=test_hooks, rng=random.Random(seed)
    )


def payment(metadata=None):
    return Payment(payment_metadata=metadata or {})


async def test_success_rate_is_about_ninety_percent():
    gw = gateway()
    results = [await gw.charge(payment()) for _ in range(5000)]
    rate = sum(r.succeeded for r in results) / len(results)
    assert 0.88 < rate < 0.92


async def test_declined_charge_has_reason():
    result = await gateway(success_rate=0).charge(payment())
    assert not result.succeeded
    assert result.failure_reason


async def test_hooks_are_ignored_unless_enabled():
    result = await gateway(success_rate=1).charge(payment({"emulator": {"result": "failed"}}))
    assert result.succeeded


async def test_hooks_force_the_outcome():
    gw = gateway(success_rate=1, test_hooks=True)
    assert not (await gw.charge(payment({"emulator": {"result": "failed"}}))).succeeded
    gw = gateway(success_rate=0, test_hooks=True)
    assert (await gw.charge(payment({"emulator": {"result": "succeeded"}}))).succeeded


async def test_default_settings_match_the_task(monkeypatch):
    for name in ("GATEWAY_MIN_DELAY", "GATEWAY_MAX_DELAY", "GATEWAY_SUCCESS_RATE"):
        monkeypatch.delenv(name, raising=False)
    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr("app.gateway.asyncio.sleep", fake_sleep)
    gw = EmulatedGateway.from_settings(Settings(api_key="x", _env_file=None))
    results = [await gw.charge(payment()) for _ in range(2000)]

    # Задержки заполняют весь диапазон 2-5 c, а не его часть.
    assert 2 <= min(delays) < 2.1
    assert 4.9 < max(delays) <= 5
    assert 0.87 < sum(r.succeeded for r in results) / len(results) < 0.93


@pytest.mark.parametrize(
    "overrides",
    [
        {"api_key": ""},
        {"api_key": "ключ"},
        {"gateway_min_delay": 6, "gateway_max_delay": 5},
        {"gateway_success_rate": 1.5},
        {"outbox_batch_size": 0},
        {"consumer_prefetch": 0},
        {"webhook_deadline": float("inf")},
    ],
)
def test_invalid_settings_are_rejected(overrides):
    with pytest.raises(ValueError):
        Settings(**{"api_key": "x", "_env_file": None, **overrides})
