#!/bin/bash

source_file=$1
destination="s3://aws-sam-cli-managed-default-samclisourcebucket-9k9zwqsha4fo/schemas/$(basename -s .json $source_file).${GITHUB_SHA}.json"
echo "uploading $source_file to $destination"

jq --null-input \
    --rawfile schema_contents $source_file \
    '{SchemaDefinition: $schema_contents}' | \
    aws s3 cp - $destination