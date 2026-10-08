import pytest

from app.messaging import (
    MAX_ATTEMPTS,
    dead_letter_queue,
    header_int,
    new_queue,
    retry_queue_for,
    retry_queues,
)


def test_three_attempts_with_growing_delay():
    assert MAX_ATTEMPTS == 3
    assert retry_queue_for(2) is retry_queues[0]
    assert retry_queue_for(4) is retry_queues[1]

    ttls = [queue.arguments["x-message-ttl"] for queue in retry_queues]
    assert ttls == [2000, 4000]


@pytest.mark.parametrize(("delay", "expected"), [(0.3, "2s"), (2, "2s"), (2.5, "4s"), (4, "4s"), (15, "4s")])
def test_retry_queue_for_delay(delay, expected):
    assert retry_queue_for(delay).name == f"payments.retry.{expected}"


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
    [
        ({}, 1),
        ({"x-attempt": "2"}, 2),
        ({"x-attempt": 3}, 3),
        ({"x-attempt": "0"}, 1),
        ({"x-attempt": "x"}, 1),
    ],
)
def test_header_int(headers, expected):
    assert header_int(headers, "x-attempt", 1) == expected
