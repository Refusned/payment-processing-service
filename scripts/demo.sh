#!/usr/bin/env sh
# Сценарий из README одной командой: создать платёж, повторить запрос,
# дождаться обработки и посмотреть, что пришло на webhook.
set -eu

API="http://127.0.0.1:${API_PORT:-8000}"
RECEIVER="http://127.0.0.1:${WEBHOOK_RECEIVER_PORT:-9000}"
KEY="${API_KEY:-dev-api-key}"
IDEMPOTENCY_KEY="demo-$(date +%s)"
BODY='{"amount": "1500.00", "currency": "RUB", "description": "Order A-1001", "metadata": {"order_id": "A-1001"}, "webhook_url": "http://webhook-receiver:9000/ok"}'

echo "== POST /api/v1/payments"
response=$(curl -fsS -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" -H "Idempotency-Key: $IDEMPOTENCY_KEY" -H "Content-Type: application/json" \
  -d "$BODY")
echo "$response"
payment_id=$(echo "$response" | sed -E 's/.*"payment_id":"([^"]+)".*/\1/')

echo
echo "== Same request with the same Idempotency-Key"
curl -sS -i -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" -H "Idempotency-Key: $IDEMPOTENCY_KEY" -H "Content-Type: application/json" \
  -d "$BODY" | grep -iE '^(HTTP|idempotent-replayed)|payment_id'

echo
echo "== Waiting for processing (2-5 s)"
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  payment=$(curl -fsS "$API/api/v1/payments/$payment_id" -H "X-API-Key: $KEY")
  case "$payment" in
    *'"webhook_status":"pending"'*) sleep 1 ;;
    *) break ;;
  esac
done
echo "$payment"

echo
echo "== Webhooks received for this payment"
curl -fsS "$RECEIVER/received?payment_id=$payment_id"
echo
