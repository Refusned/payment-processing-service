TEST_COMPOSE = docker compose -p payments-test --env-file tests/integration/test.env

.PHONY: up down logs demo lint test-unit test-integration

up:
	docker compose up -d --build --wait

down:
	docker compose down

logs:
	docker compose logs -f api consumer

demo:
	./scripts/demo.sh

lint:
	ruff check .
	ruff format --check .
	mypy app tools

test-unit:
	pytest tests/unit

# Отдельный compose-проект со своими портами и томами, рабочий стек не трогает.
# Параллельно два прогона не запускать: у них общее имя проекта и порты.
test-integration:
	@trap 'status=$$?; $(TEST_COMPOSE) down -v || status=1; exit $$status' EXIT; \
	$(TEST_COMPOSE) up -d --build --wait && pytest tests/integration -v
