
# Removing sensitive values from datasets (PII)

There are a variety of options for dealing with PII when building data pipelines depending on the use case.

## Methods

### Removal

The first and simplest solution is to remove the fields containing PII altogether. Make sure your data product needs to include the sensitive data before going through the effort of implementing one of the solutions below.

### Encryption

The second option is to encrypt the sensitive data and store the encoded ciphertext in your dataset. This solution is suitable when the sensitive values are larger, have few repeated values, and do not need to be joined to other tables or datasets, i.e. an email body. The [AWS Encryption SDK](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/introduction.html) can be used with a KMS key to encrypt the data using an envelope encryption method. Note that values encrypted with the AWS Encryption SDK are most easily decrypted using the SDK due to this proprietary method. By default, the SDK generates a new data key for each value encrypted via a network call. When encrypting large datasets it is important to configure a [cache](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/data-key-caching.html) to reuse data keys and reduce network calls.

There are several ways to integrate this encryption process into your ETL. If the data is processed in small enough batches, the `collect` function can be used on your dataframe to bring the necessary data into the memory of the driver node to encrypt the data using the SDK. This method is very simple and suitable when the data is processed in batches of less than 1 GB. If the data is too large to process on the driver node, a Step Function can be used to parallelize the decryption work using Lambdas.

### Tokenization

When the sensitive data is small, will be encrypted many times as new data comes in, and can be used to join datasets, then tokenization is a good option. Tokenization involves encrypting the data and assigning it a unique identifier that will act as the token before storing it in a database such as DynamoDB. Subsequent runs should check to see if the value has been tokenized before creating a new token. This can be implemented as a separate service and shared among teams.

### Redaction

Redaction is used when the majority of the value is desired, but the value can contain sensitive data. [AWS Comprehend](https://aws.amazon.com/comprehend/) has features to do PII detection and removal. Type of PII can be specified, for example, SSN, Name, or Credit Card Numbers. The redacted text can be replaced with the type of PII that was removed. This can be integrated with Athena for ease of use ([tutorial](https://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/)).
