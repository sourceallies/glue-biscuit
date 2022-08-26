from amazon/aws-glue-libs:glue_libs_3.0.0_image_01
add ./src ./src
add ./test_data ./test_data
ENV PYTHONPATH="${PYTHONPATH}:./src"
RUN ["pip3", "install", "pytest", "pytest-mock", "cfn-flip"]
ENTRYPOINT ["python3", "-m", "pytest", "./src"]
