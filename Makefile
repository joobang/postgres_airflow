# https://gitlab.com/gitlab-data/analytics/blob/master/Makefile
.PHONY: up build test help default

default: build

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

build: ## Build airflow
	@echo "Building Airflow image"
	@docker-compose build webserver scheduler

up: ## Build and start airflow
	@echo "Booting up airflow"
	@docker-compose up --build initdb
	docker-compose run --name airflow-create-user --rm webserver \
		./wait-for-it.sh postgres:5432 -t 10 -- airflow users create -e admin@example.org -f admin -l admin -p admin -r Admin -u admin \
		&& echo 'User is created. ID: admin, PW: admin'
	@docker-compose up --build webserver scheduler triggerer

reset-data: ## Reset all analytics data
	@docker stop $$(docker ps -a --filter "name=postgres_analytics" -q)
	@docker rm $$(docker ps -a --filter "name=postgres_analytics" -q)
	@docker volume rm ehwnghks-gmailcom_postgres_analytics_data

	@docker stop $$(docker ps -a --filter "name=postgres_airflow" -q)
	@docker rm $$(docker ps -a --filter "name=postgres_airflow" -q)
	@docker volume rm ehwnghks-gmailcom_postgres_airflow_data
