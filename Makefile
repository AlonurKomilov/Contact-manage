PYTHON := .venv/bin/python
PIP := .venv/bin/pip
PID_FILE := .runtime/bot.pid
LOG_FILE := .runtime/bot.log

.PHONY: install start stop restart status logs env-check

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@mkdir -p .runtime

env-check:
	@test -f .env || (echo ".env not found. Create it from .env.example" && exit 1)
	@grep -Eq '^BOT_TOKEN=.+$$' .env || (echo "BOT_TOKEN is empty in .env" && exit 1)
	@grep -Eq '^TG_API_ID=[0-9]+$$' .env || (echo "TG_API_ID must be a non-empty integer in .env" && exit 1)
	@grep -Eq '^TG_API_HASH=.+$$' .env || (echo "TG_API_HASH is empty in .env" && exit 1)
	@grep -Eq '^SESSION_SECRET=.+$$' .env || (echo "SESSION_SECRET is empty in .env" && exit 1)

start: env-check
	@mkdir -p .runtime
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Bot is already running with PID $$(cat $(PID_FILE))"; \
		exit 1; \
	fi
	@nohup $(PYTHON) main.py >> $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@sleep 2
	@if kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Bot started with PID $$(cat $(PID_FILE))"; \
	else \
		echo "Bot failed to start. Recent log output:"; \
		tail -n 40 $(LOG_FILE); \
		rm -f $(PID_FILE); \
		exit 1; \
	fi

stop:
	@if [ ! -f $(PID_FILE) ]; then \
		echo "Bot is not running"; \
		exit 0; \
	fi
	@if kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		kill $$(cat $(PID_FILE)); \
		echo "Bot stopped"; \
	else \
		echo "Stale PID file removed"; \
	fi
	@rm -f $(PID_FILE)

restart: stop start

status:
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Bot is running with PID $$(cat $(PID_FILE))"; \
	else \
		echo "Bot is not running"; \
	fi

logs:
	@mkdir -p .runtime
	@touch $(LOG_FILE)
	tail -n 100 -f $(LOG_FILE)