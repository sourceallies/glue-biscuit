
get-pyglue-libs:
	curl -o PyGlue.zip https://s3.amazonaws.com/aws-glue-jes-prod-us-east-1-assets/etl-1.0/python/PyGlue.zip

pip-install:
	pip install -r requirements.txt

setup-env:
	grep -c "PyGlue.zip" .env > /dev/null || echo "PYTHONPATH=.:$$(pwd)/PyGlue.zip" >> .env

setup-local: get-pyglue-libs pip-install setup-env
	echo 'Local setup done'