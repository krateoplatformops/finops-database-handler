REPO?=#your repository here
VERSION?=0.1

container:
	docker build -t $(REPO)finops-database-handler:$(VERSION) .
	docker push $(REPO)finops-database-handler:$(VERSION)

container-multi:
	docker buildx build --tag $(REPO)finops-database-handler:$(VERSION) --push --platform linux/amd64,linux/arm64 .