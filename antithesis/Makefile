.PHONY: client broker down up

client:
	podman build -f client/Dockerfile -t us-central1-docker.pkg.dev/molten-verve-216720/bytebeam-repository/rumqttc:latest ../

broker:
	podman build -f broker/Dockerfile -t us-central1-docker.pkg.dev/molten-verve-216720/bytebeam-repository/rumqttd:latest ../

down:
	docker-compose -f config/docker-compose.yaml down

up: down
	docker-compose -f config/docker-compose.yaml up