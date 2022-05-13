from amazon/aws-glue-libs:glue_libs_3.0.0_image_01
ENV PYTHONPATH="${PYTHONPATH}:./src"
RUN ["pip", "install", "pytest"]
ENTRYPOINT ["python3"]
CMD ["./src/glue_api_example.py"]
