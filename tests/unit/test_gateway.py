import random

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
