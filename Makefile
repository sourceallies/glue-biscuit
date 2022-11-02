
get-pyglue-libs:
	curl -o PyGlue.zip https://s3.amazonaws.com/aws-glue-jes-prod-us-east-1-assets/etl-1.0/python/PyGlue.zip

pip-install:
	pip install -r requirements.txt

setup-env:
	grep -c "PyGlue.zip" .env > /dev/null || echo "PYTHONPATH=.:$$(pwd)/PyGlue.zip" >> .env

setup-local: get-pyglue-libs pip-install setup-env
	echo 'Local setup done'

lint:
	black --check --diff ./src

format:
	black ./src

run-unit-tests:
	docker run \
		-v "$$(pwd):/work" \
		-e DISABLE_SSL=true \
		-e PYTHONPATH='/home/glue_user/aws-glue-libs/PyGlue.zip:/home/glue_user/spark/python/lib/py4j-0.10.9-src.zip:/home/glue_user/spark/python/:/work/src' \
		-w /work \
		--entrypoint=/work/docker-unit-tests.sh \
		amazon/aws-glue-libs:glue_libs_3.0.0_image_01


run-glue-container:
	docker run -it \
		-v "$$HOME/.aws:/home/glue_user/.aws" \
		-v "$$(pwd):/work" \
		-e AWS_DEFAULT_REGION=us-east-1 \
		-e DISABLE_SSL=true \
		-e PYTHONPATH=/work/src \
		-w /work \
		--entrypoint=bash \
		amazon/aws-glue-libs:glue_libs_3.0.0_image_01

save-aws-credentials:
	@aws configure set --profile=$(profile) aws_access_key_id $(AWS_ACCESS_KEY_ID)
	@aws configure set --profile=$(profile) aws_secret_access_key $(AWS_SECRET_ACCESS_KEY)
	@aws configure set --profile=$(profile) aws_session_token $(AWS_SESSION_TOKEN)
	@aws configure set --profile=$(profile) default.region us-east-1
	@echo "to set the profile in the container: export AWS_DEFAULT_PROFILE=$(profile); export AWS_PROFILE=$(profile)"
