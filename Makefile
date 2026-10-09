PY := cd python && uv run python

.PHONY: setup test lint simulator gateway scenario1 scenario2 scenario3 scenarios verify

setup:            ## install dependencies (needs uv)
	uv sync --project python

test:             ## unit + integration tests
	cd python && uv run pytest -q

lint:
	cd python && uvx ruff check .

simulator:        ## run the provider simulator on :9000
	$(PY) -m simulator --port 9000

gateway:          ## run the gateway on :8000 against the simulator on :9000
	$(PY) -m gateway --port 8000 --provider-url http://127.0.0.1:9000

scenario1:        ## 50k RPM capacity test (~9 min)
	$(PY) -m scenarios ../config/scenarios/scenario1.yaml $(ARGS)

scenario2:        ## two models, limit drop and restore (~10 min)
	$(PY) -m scenarios ../config/scenarios/scenario2.yaml $(ARGS)

scenario3:        ## async batch + flaky callback (~1 min)
	$(PY) -m scenarios ../config/scenarios/scenario3.yaml $(ARGS)

scenarios: scenario1 scenario2 scenario3

verify:           ## re-verify a run: make verify RUN=runs/<dir>
	$(PY) -m verify ../$(RUN)
