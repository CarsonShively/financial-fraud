.PHONY: venv install redis-up redis-down redis-ping demo parity data train promote

SHELL := /bin/bash

REDIS_HOST ?= 127.0.0.1
REDIS_PORT ?= 6380

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
UV   := $(VENV)/bin/uv

STREAMLIT_APP ?= app/demo.py

UPLOAD ?= 0
PROMOTE ?= 0

ROLE ?= baseline
MODEL ?= lr

venv:
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip uv

install: venv
	$(UV) sync

install-dev: venv
	$(UV) sync --extra dev

lock: venv
	$(UV) lock

redis-up:
	@redis-server --bind $(REDIS_HOST) --port $(REDIS_PORT) --save "" --appendonly no --daemonize yes
	@redis-cli -h $(REDIS_HOST) -p $(REDIS_PORT) ping

redis-ping:
	@redis-cli -h $(REDIS_HOST) -p $(REDIS_PORT) ping

redis-down:
	@redis-cli -h $(REDIS_HOST) -p $(REDIS_PORT) shutdown || true

demo:
	@$(PY) -m streamlit run $(STREAMLIT_APP)

parity:
	@$(PY) parity/test.py

data: ## Run data job (UPLOAD=1 to upload)
	@$(PY) jobs/10_data.py $(if $(filter 1,$(UPLOAD)),--upload,)

train: ## Train (MODEL=lr|lgb|xgb) (ROLE=baseline|candidate) (UPLOAD=1 to upload)
	@$(PY) jobs/20_train.py \
		--model $(MODEL) \
		--role $(ROLE) \
		$(if $(filter 1,$(UPLOAD)),--upload,)

promote: ## Promotion job (PROMOTE=1 to promote)
	@$(PY) jobs/30_promotion.py $(if $(filter 1,$(PROMOTE)),--promote,)
