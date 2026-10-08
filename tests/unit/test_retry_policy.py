import pytest

from app.messaging import (
    MAX_ATTEMPTS,
    attempt_of,
    dead_letter_queue,
    new_queue,
    retry_queue_after,
    retry_queues,
)


def test_three_attempts_with_growing_delay():
    assert MAX_ATTEMPTS == 3
    assert retry_queue_after(1) is retry_queues[0]
    assert retry_queue_after(2) is retry_queues[1]
    assert retry_queue_after(3) is None

    ttls = [queue.arguments["x-message-ttl"] for queue in retry_queues]
    assert ttls == [2000, 4000]


def test_retry_queues_return_messages_to_work_queue():
    for queue in retry_queues:
        assert queue.arguments["x-dead-letter-exchange"] == "payments"
        assert queue.arguments["x-dead-letter-routing-key"] == new_queue.routing_key


def test_work_queue_dead_letters_into_dlq():
    assert new_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert new_queue.arguments["x-dead-letter-routing-key"] == dead_letter_queue.routing_key


@pytest.mark.parametrize("queue", [new_queue, *retry_queues])
def test_dead_lettering_is_at_least_once(queue):
    assert queue.arguments["x-queue-type"] == "quorum"
    assert queue.arguments["x-dead-letter-strategy"] == "at-least-once"
    assert queue.arguments["x-overflow"] == "reject-publish"


@pytest.mark.parametrize(
    ("headers", "expected"),
    [({}, 1), ({"x-attempt": "2"}, 2), ({"x-attempt": 3}, 3), ({"x-attempt": "0"}, 1), ({"x-attempt": "x"}, 1)],
)
def test_attempt_of(headers, expected):
    assert attempt_of(headers) == expected
