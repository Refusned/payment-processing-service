#!/usr/bin/env sh
# Сценарий из README одной командой: создать платёж, повторить запрос, дождаться
# обработки и убедиться, что webhook дошёл. При любом расхождении выход с кодом 1.
set -eu
cd "$(dirname "$0")/.."

# Те же порты и ключ, что видит docker compose.
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

API="http://127.0.0.1:${API_PORT:-8000}"
RECEIVER="http://127.0.0.1:${WEBHOOK_RECEIVER_PORT:-9000}"
KEY="${API_KEY:-dev-api-key}"
IDEMPOTENCY_KEY="demo-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
BODY='{"amount": "1500.00", "currency": "RUB", "description": "Order A-1001", "metadata": {"order_id": "A-1001"}, "webhook_url": "http://webhook-receiver:9000/ok"}'

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

post_payment() {
  curl -sS --max-time 10 -i -X POST "$API/api/v1/payments" \
    -H "X-API-Key: $KEY" -H "Idempotency-Key: $IDEMPOTENCY_KEY" -H "Content-Type: application/json" \
    -d "$BODY"
}

echo "== POST /api/v1/payments"
first=$(post_payment)
echo "$first" | tail -n1
echo "$first" | grep -q '^HTTP/[0-9.]* 202' || fail "expected 202 Accepted"
payment_id=$(echo "$first" | tail -n1 | sed -nE 's/.*"payment_id":"([^"]+)".*/\1/p')
[ -n "$payment_id" ] || fail "no payment_id in the response"

echo
echo "== Same request with the same Idempotency-Key"
replay=$(post_payment)
echo "$replay" | grep -iE '^(HTTP|idempotent-replayed)'
echo "$replay" | grep -q '^HTTP/[0-9.]* 202' || fail "expected 202 Accepted on replay"
echo "$replay" | grep -qi '^idempotent-replayed: true' || fail "no Idempotent-Replayed header"
echo "$replay" | grep -q "\"payment_id\":\"$payment_id\"" || fail "replay returned a different payment"

echo
echo "== Waiting for processing (2-5 s)"
payment=""
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  payment=$(curl -fsS --max-time 10 "$API/api/v1/payments/$payment_id" -H "X-API-Key: $KEY")
  case "$payment" in
    *'"webhook_status":"pending"'*) sleep 1 ;;
    *) break ;;
  esac
done
echo "$payment"
case "$payment" in
  *'"webhook_status":"delivered"'*) ;;
  *) fail "webhook was not delivered within 20 s" ;;
esac

echo
echo "== Webhooks received for this payment"
received=$(curl -fsS --max-time 10 "$RECEIVER/received?payment_id=$payment_id")
echo "$received"
echo "$received" | grep -q "\"payment_id\":\"$payment_id\"" || fail "receiver got nothing"

echo
echo "OK"
