REPO?=#your repository here
VERSION?=0.1

container:
	docker build -t $(REPO)finops-database-handler:$(VERSION) .
	docker push $(REPO)finops-database-handler:$(VERSION)