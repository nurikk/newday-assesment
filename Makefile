VERSION=$(strip $(shell cat VERSION))
REPO_NAME=$(notdir $(shell pwd))
CONTAINER_NAME=spark-app

help:
	@echo "startdevenv - start dev environment"
	@echo "unittest    - run unit test in container"
	@echo "build       - package dependencies into a PEX binary"
	@echo "test-submit - test spark-submit a sample job"
	@echo "bash        - run bash in the container"

stopdevenv:
	@docker-compose -f docker-compose.yaml down

startdevenv: stopdevenv
	@docker-compose -f docker-compose.yaml up --build -d

bash:
	@docker exec -it $(CONTAINER_NAME) /bin/bash

build:
	@rm -rf ./dist;
	@docker exec $(CONTAINER_NAME) ./build-reqs.sh

test-submit: build
	@docker exec $(CONTAINER_NAME) ./submit.sh

unittest: build
	@docker exec -it $(CONTAINER_NAME) /bin/bash -c "python3 -m pytest -s --disable-warnings test/"