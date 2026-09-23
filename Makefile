.PHONY: help init plan apply destroy clean format lint test doctor smoke

help:
	@echo "AWS MSK Kafka Starter - Makefile Commands"
	@echo ""
	@echo "Terraform Commands:"
	@echo "  make init      - Initialize Terraform"
	@echo "  make plan      - Preview infrastructure changes"
	@echo "  make apply     - Deploy infrastructure"
	@echo "  make destroy   - Destroy all infrastructure"
	@echo "  make outputs   - Show Terraform outputs"
	@echo ""
	@echo "Development Commands:"
	@echo "  make format    - Format Terraform and Python code"
	@echo "  make lint      - Run linters"
	@echo "  make test      - Run all tests and checks"
	@echo "  make clean     - Clean temporary files"
	@echo ""
	@echo "Health & Testing Commands:"
	@echo "  make doctor    - Run health checks for AWS and MSK setup"
	@echo "  make smoke     - Run end-to-end smoke test (requires deployed infra)"
	@echo ""
	@echo "Kafka Commands:"
	@echo "  make setup-env - Export Terraform outputs as environment variables"
	@echo "  make topic     - Create Kafka topic"
	@echo "  make produce   - Run producer (produces 10 messages)"
	@echo "  make consume   - Run consumer"

init:
	cd infra && terraform init

plan:
	cd infra && terraform plan

apply:
	cd infra && terraform apply

destroy:
	cd infra && terraform destroy

outputs:
	cd infra && terraform output

format:
	cd infra && terraform fmt -recursive
	black scripts/
	isort scripts/

lint:
	cd infra && terraform fmt -check -recursive
	cd infra && terraform validate
	black --check scripts/
	isort --check-only scripts/
	pylint scripts/*.py --disable=C0114,C0116 --max-line-length=100 || true

test: lint
	python -m py_compile scripts/*.py
	@echo "All tests passed!"

clean:
	rm -rf infra/.terraform
	rm -f infra/.terraform.lock.hcl
	rm -f infra/terraform.tfstate*
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

setup-env:
	@echo "Run the following commands to set environment variables:"
	@echo ""
	@echo "export BOOTSTRAP_SERVERS=\$$(cd infra && terraform output -raw msk_bootstrap_brokers)"
	@echo "export KAFKA_TOPIC=\$$(cd infra && terraform output -raw kafka_topic_name)"
	@echo "export AWS_REGION=\$$(cd infra && terraform output -raw aws_region)"

topic:
	@if [ -z "$$BOOTSTRAP_SERVERS" ]; then \
		echo "Error: BOOTSTRAP_SERVERS not set. Run 'make setup-env' first."; \
		exit 1; \
	fi
	python scripts/create_topic.py \
		--bootstrap-servers "$$BOOTSTRAP_SERVERS" \
		--topic "$$KAFKA_TOPIC" \
		--region "$$AWS_REGION"

produce:
	@if [ -z "$$BOOTSTRAP_SERVERS" ]; then \
		echo "Error: BOOTSTRAP_SERVERS not set. Run 'make setup-env' first."; \
		exit 1; \
	fi
	python scripts/produce.py \
		--bootstrap-servers "$$BOOTSTRAP_SERVERS" \
		--topic "$$KAFKA_TOPIC" \
		--count 10 \
		--region "$$AWS_REGION"

consume:
	@if [ -z "$$BOOTSTRAP_SERVERS" ]; then \
		echo "Error: BOOTSTRAP_SERVERS not set. Run 'make setup-env' first."; \
		exit 1; \
	fi
	python scripts/consume.py \
		--bootstrap-servers "$$BOOTSTRAP_SERVERS" \
		--topic "$$KAFKA_TOPIC" \
		--from-beginning \
		--region "$$AWS_REGION"

doctor:
	python scripts/doctor.py

smoke:
	@if [ -z "$$BOOTSTRAP_SERVERS" ]; then \
		echo "Error: BOOTSTRAP_SERVERS not set. Run 'make setup-env' first."; \
		exit 1; \
	fi
	python scripts/smoke_test.py \
		--bootstrap-servers "$$BOOTSTRAP_SERVERS" \
		--topic "$$KAFKA_TOPIC" \
		--region "$$AWS_REGION"
