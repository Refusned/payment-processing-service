import os

# Настройки читаются при импорте app.config, ключ нужен до первого импорта приложения.
os.environ.setdefault("API_KEY", "test-api-key")
