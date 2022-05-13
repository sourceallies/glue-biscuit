
get-pyglue-libs:
	curl -o PyGlue.zip https://s3.amazonaws.com/aws-glue-jes-prod-us-east-1-assets/etl-1.0/python/PyGlue.zip

pip-install:
	pip install -r requirements.txt

setup-env:
	grep -c "PyGlue.zip" .env > /dev/null || echo "PYTHONPATH=.:$$(pwd)/PyGlue.zip" >> .env

setup-local: get-pyglue-libs pip-install setup-env
	echo 'Local setup done'

docker-build:
	docker build -t glue .

run-api-example: 
	docker run -v "$$(pwd)/src:/src" -w /src -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_DEFAULT_REGION glue glue_api_example.py

test: 
	docker run -v "$$(pwd)/src:/src" -w /src --entrypoint pytest glue 
