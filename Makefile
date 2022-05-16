
get-pyglue-libs:
	curl -o PyGlue.zip https://s3.amazonaws.com/aws-glue-jes-prod-us-east-1-assets/etl-1.0/python/PyGlue.zip

pip-install:
	pip install -r requirements.txt

setup-env:
	grep -c "PyGlue.zip" .env > /dev/null || echo "PYTHONPATH=.:$$(pwd)/PyGlue.zip" >> .env

setup-local: get-pyglue-libs pip-install setup-env
	echo 'Local setup done'

run-example-job:
	docker run -it \
		-v "$$(pwd)/src:/src" \
		-e AWS_DEFAULT_REGION=us-east-1 \
		-e AWS_ACCESS_KEY_ID \
		-e AWS_SECRET_ACCESS_KEY \
		-e AWS_SESSION_TOKEN \
		-w /src \
		--entrypoint=/home/glue_user/spark/bin/pyspark \
		sha256:42fb92b99d96a201d5447034ea06479a7cd61266dd4af7afeee704283d46fcc7 \
		example_job.py

lint:
	flake8 ./src

format:
	black ./src

docker-build:
	docker build -t glue .

test:
	docker run -v "$$(pwd)/src:/src" -w /src --entrypoint pytest glue
