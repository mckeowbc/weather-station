
VERSION=v0.3
REGISTRY=registry.mckeownlab.diy

prometheus-docker:
	@ docker build -f Promdocker -t ${REGISTRY}/prometheus-proxy:${VERSION} .

pws-docker:
	@ docker build -f PWSdocker -t ${REGISTRY}/pws-publisher:${VERSION} .

push-pws: pws-docker
	docker push ${REGISTRY}/pws-publisher:${VERSION}

push-prometheus: prometheus-docker
	docker push ${REGISTRY}/prometheus-proxy:${VERSION}
