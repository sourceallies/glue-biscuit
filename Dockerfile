from amazon/aws-glue-libs:glue_libs_3.0.0_image_01
add ./src ./src
ENV PYTHONPATH="${PYTHONPATH}:./src"
RUN ["pip", "install", "pytest", "pytest-mock"]
ENTRYPOINT ["pytest", "./src"]
