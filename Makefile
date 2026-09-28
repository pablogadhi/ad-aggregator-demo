SHELL := /usr/bin/env bash
ENV   := source scripts/env.sh &&

.DEFAULT_GOAL := help
.PHONY: help doctor up down nuke dev ci smoke test e2e load chaos chaos-clear graph new-service new-design harvest

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

doctor: ## read-only check of tools, docker and kernel limits
	@scripts/doctor.sh

up: ## create cluster + platform + stack.yaml components (idempotent)
	@scripts/up.sh

down: ## delete the cluster (keeps the image registry cache)
	@scripts/down.sh

nuke: ## delete the cluster and the local image registry
	@scripts/down.sh --all

dev: ## Tilt UI: build + deploy services/client with live rebuilds (http://localhost:10350)
	@$(ENV) tilt up

ci: ## Tilt headless: build + deploy everything, exit non-zero if anything is unhealthy
	@$(ENV) tilt ci --timeout 15m

smoke: ## smoke-test components (C=<name> for one)
	@scripts/smoke-all.sh $(C)

test: ## unit tests: all services + repo scripts (no cluster needed)
	@cd services && uv run --all-packages pytest -q
	@python3 -m unittest discover -s scripts/tests -q

e2e: ## acceptance flows against the live cluster
	@$(ENV) cd tests/e2e && { uv run pytest -q; rc=$$?; [ $$rc -eq 5 ] && echo "no e2e tests yet" && rc=0; exit $$rc; }

load: ## in-cluster k6 load test via k6-operator (S=<scenario: clicks|hot|scale|chaos>, RUN=<id>); loadtest/run.sh
	@$(ENV) loadtest/run.sh $(or $(S),clicks)

chaos: ## apply a chaos experiment (E=<chaos/*.yaml name, without .yaml>)
	@$(ENV) kubectl --context $$SDL_CLUSTER apply -f chaos/$(E).yaml

chaos-clear: ## remove all chaos experiments
	@$(ENV) kubectl --context $$SDL_CLUSTER delete -f chaos/ --ignore-not-found

graph: ## parse design/diagram.excalidraw -> design/diagram.graph.md
	@python3 scripts/excalidraw_to_graph.py design/diagram.excalidraw -o design/diagram.graph.md

new-service: ## scaffold services/<N> from services/_template (N=<name>)
	@scripts/new-service.sh $(N)

new-design: ## clone this template into ../<N> (N=<name> [ARGS="--diagram f.excalidraw --png f.png"])
	@scripts/new-design.sh $(N) $(ARGS)

harvest: ## push infra/components/<C> back to the template as branch component/<C>
	@scripts/harvest-component.sh $(C)
