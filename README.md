# dynamotest

Use the power of [DynamoDB Local][1] with [ory/dockertest][2], create your DynamoDB test cases with breeze.

[1]: https://hub.docker.com/r/amazon/dynamodb-local/
[2]: https://github.com/ory/dockertest

## 🌄 What is `dynamotest`?

`dynamotest` is a package to help set up a DynamoDB Local Docker instance on your machine as a part of Go test code. It uses [`ory/dockertest`][2] to start the DynamoDB Local instance in your Go test code, and is configured so that each call to `dynamotest.NewDynamoDB(t)` will create a dedicated instance to allow parallel testing on multiple Docker instances. The function returns a new DynamoDB client which is already connected to the instance, and thus you can simply start using the client straight away. Also, it provides the clean up function to ensure that the Docker instance gets deleted if clean-up is preferred. If you do not call the clean up function, the instance will keep running, which may be useful for debugging and investigation.

`dynamotest` also provides a helper function `dynamotest.PrepTable(t, client, ...dynamotest.InitialTableSetup)` to prepare tables and dataset for setting up the table before hand.

It is also worth noting how this package uses only the v2 version of AWS SDK.

**NOTE**: It is a prerequisite that you are able to start up Docker container for DynamoDB Local.

## 🚀 Examples

### Minimal Setup Overview

```go
func TestMinimalSetup(t *testing.T) {
	// Create a new DynamoDB Local instance. Second return value can be called
	// to delete the instance.
	client, clean := dynamotest.NewDynamoDB(t)
	defer clean()

	// Prepare table with some data. You can provide as many data set as you
	// need by providing `dynamotest.InitialTableSetup` structs.
	dynamotest.PrepTable(t, client,
		dynamotest.InitialTableSetup{
			Table: &dynamodb.CreateTableInput{
				// Table definition.
			},
			InitialData: []*types.PutRequest{
				// Initial data to populate the table with.
			},
		},
		// Provide as many initial data as you need
	)

	// Now the DynamoDB client, which is connected to the test instance, is
	// ready to be used.
	_ = client
}
```

Ref: [minimal_test.go](/minimal_test.go)
