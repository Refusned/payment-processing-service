import asyncio
import random
from dataclasses import dataclass
from typing import Protocol

from app.config import Settings
from app.models import Payment

TEST_HOOK_KEY = "emulator"


@dataclass(frozen=True)
class ChargeResult:
    succeeded: bool
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> ChargeResult: ...


class EmulatedGateway:
    """Эмуляция внешнего платёжного шлюза: ответ через 2-5 с, 90% успешных списаний.

    Настоящий шлюз получал бы id платежа как ключ идемпотентности, и повторный вызов
    для того же платежа не списал бы деньги второй раз.
    """

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        test_hooks: bool = False,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._test_hooks = test_hooks
        self._rng = rng or random.Random()

    @classmethod
    def from_settings(cls, settings: Settings) -> "EmulatedGateway":
        return cls(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
            test_hooks=settings.gateway_test_hooks,
        )

    async def charge(self, payment: Payment) -> ChargeResult:
        delay = self._rng.uniform(self._min_delay, self._max_delay)
        succeeded = self._rng.random() < self._success_rate

        if self._test_hooks:
            # {"emulator": {"result": "failed", "delay": 0.1}} в metadata делает исход предсказуемым.
            hook = payment.payment_metadata.get(TEST_HOOK_KEY)
            if isinstance(hook, dict):
                delay = float(hook.get("delay", delay))
                if hook.get("result") in ("succeeded", "failed"):
                    succeeded = hook["result"] == "succeeded"

        await asyncio.sleep(delay)
        if succeeded:
            return ChargeResult(succeeded=True)
        return ChargeResult(succeeded=False, failure_reason="Declined by payment gateway")
