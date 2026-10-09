PY := cd python && uv run python

.PHONY: setup test lint simulator gateway scenario1 scenario2 scenario3 scenarios verify

setup:            ## install dependencies (needs uv)
	uv sync --project python

test:             ## Python unit + integration tests
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

GO := cd go && go run ./cmd/bench

.PHONY: go-test bench-300k bench-300k-spread bench-1m bench-ceiling bench-charts

go-test:          ## Go limiter + engine tests
	cd go && go vet ./... && go test ./...

bench-300k:       ## required: 300k req/s, 64 models (3 runs x 75 s)
	$(GO) -target 300000 -layout pinned

bench-300k-spread: ## required: 300k req/s, one model split over every shard
	$(GO) -target 300000 -layout spread -models 1 -name spread-300000

bench-1m:         ## stretch: 1M req/s
	$(GO) -target 1000000 -layout pinned

bench-ceiling:    ## limits disabled: 11M req/s offered
	$(GO) -target 10000000 -ceiling -generators 4

bench-charts:     ## docs/results/go/throughput.png from runs/go/*.json
	$(PY) -m verify.bench_charts ../runs/go ../docs/results/go
