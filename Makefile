IMAGE_PREFIX = reynoldsm88
IMAGE_NAME = odinson-extra
IMG := $(IMAGE_PREFIX)/$(IMAGE_NAME):latest

docker-build:
#	sbt "project extra" "dist"
	docker build -t $(IMG) .

docker-push: docker-build
	docker push $(IMG)