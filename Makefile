PROJECT_NAME = notebook_r
JUPYTER_IMAGE := $(PROJECT_NAME):latest

build:
	docker build -f docker/Dockerfile --tag $(JUPYTER_IMAGE) .

start: build
	docker run -p  8888:8888 -v $(PWD)/project:/app/project $(JUPYTER_IMAGE)