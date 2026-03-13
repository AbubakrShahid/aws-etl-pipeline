.PHONY: test lint format format-check install deploy destroy upload-data run-job

TERRAFORM_DIR := infra/terraform
BUCKET ?= $(shell cd $(TERRAFORM_DIR) && terraform output -raw data_bucket_name 2>/dev/null || echo "")
JOB_NAME ?= $(shell cd $(TERRAFORM_DIR) && terraform output -raw glue_job_name 2>/dev/null || echo "")
REGION ?= us-east-1

test:
	python3 -m pytest tests/unit -v

lint:
	python3 -m flake8 glue_scripts/ tests/

format:
	python3 -m black glue_scripts/ tests/

format-check:
	python3 -m black --check glue_scripts/ tests/

install:
	pip install -r requirements-dev.txt

deploy:
	cd $(TERRAFORM_DIR) && terraform init && terraform apply -var-file=terraform.tfvars -auto-approve

destroy:
	cd $(TERRAFORM_DIR) && terraform destroy -var-file=terraform.tfvars -auto-approve

upload-data:
	@if [ -z "$(BUCKET)" ]; then echo "Error: BUCKET not set. Run 'make deploy' first or pass BUCKET=your-bucket-name"; exit 1; fi
	python3 scripts/upload_brazilian_ecommerce_dataset.py --bucket-name $(BUCKET) --region $(REGION)

run-job:
	@if [ -z "$(JOB_NAME)" ]; then echo "Error: JOB_NAME not set. Run 'make deploy' first or pass JOB_NAME=your-job-name"; exit 1; fi
	aws glue start-job-run --job-name $(JOB_NAME) --region $(REGION)
