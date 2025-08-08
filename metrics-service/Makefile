
VERSION=v0.2
REGISTRY=registry.mckeownlab.diy

prometheus-docker:
	@ docker build -f Promdocker -t ${REGISTRY}/prometheus-proxy:${VERSION} .

pws-docker:
	@ docker build -f PWSdocker -t ${REGISTRY}/pws-publisher:${VERSION} .
