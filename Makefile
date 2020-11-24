PHALANX_VERSION ?= $(shell cargo metadata --no-deps --format-version=1 | jq -r '.packages[] | select(.name=="phalanx") | .version')

clean:
	cargo clean

format:
	cargo fmt

build:
	cargo build --release

test:
	cargo test

tag:
	git tag v$(PHALANX_VERSION)
	git push origin v$(PHALANX_VERSION)

docker-build:
	docker build --tag=mosuka/phalanx:latest --file=Dockerfile --build-arg="PHALANX_VERSION=$(PHALANX_VERSION)" .
	docker tag mosuka/phalanx:latest mosuka/phalanx:$(PHALANX_VERSION)

docker-push:
	docker push mosuka/phalanx:latest
	docker push mosuka/phalanx:$(PHALANX_VERSION)

docker-clean:
ifneq ($(shell docker ps -f 'status=exited' -q),)
	docker rm $(shell docker ps -f 'status=exited' -q)
endif
ifneq ($(shell docker images -f 'dangling=true' -q),)
	docker rmi -f $(shell docker images -f 'dangling=true' -q)
endif
ifneq ($(docker volume ls -f 'dangling=true' -q),)
	docker volume rm $(docker volume ls -f 'dangling=true' -q)
endif
