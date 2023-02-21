## You can follow the steps below in order to get yourself a local ODC.
## Start by running `setup` then you should have a system that is fully configured

.PHONY: help setup up down clean

BBOX := 11,45,12,46

help: ## Print this help
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: build up init product index explorer ## Run a full local/development setup
update: build up ## Run a full local/development setup

up: ## 1. Bring up your Docker environment
	docker-compose up -d postgres
	docker-compose run checkdb
	docker-compose up -d explorer
	docker-compose up -d openeo_odc_driver

init: ## 2. Prepare the database
	docker-compose exec -T openeo_odc_driver datacube -v system init

product: ## 3. Add a product definition for Sentinel-2
	docker-compose exec -T openeo_odc_driver wget https://raw.githubusercontent.com/digitalearthafrica/config/master/products/esa_s2_l2a.odc-product.yaml
	docker-compose exec -T openeo_odc_driver datacube product add esa_s2_l2a.odc-product.yaml

index: ## 4. Index some data (Change extents with BBOX='<left>,<bottom>,<right>,<top>')
	docker-compose exec -T openeo_odc_driver bash -c \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://earth-search.aws.element84.com/v0/' \
			--collections='sentinel-s2-l2a-cogs' \
			--datetime='2015-06-01/2023-07-01'"

explorer: ## 5. Prepare the explorer
	docker-compose exec -T explorer cubedash-gen --init --all

down: ## Bring down the system
	docker-compose down

build: ## Rebuild the base image
	docker-compose pull
	docker-compose build

shell: ## Start an interactive shell
	docker-compose exec openeo_odc_driver bash

clean: ## Delete everything
	docker-compose down --rmi all -v

logs: ## Show the logs from the stack
	docker-compose logs --follow
