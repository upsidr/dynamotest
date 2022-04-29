package dynamotest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

// NewDynamoDB creates a Docker container with DynamoDB Local, and returns the
// connected DynamoDB client. Clean up function is returned as well to ensure
// container gets removed after test is complete.
func NewDynamoDB(t testing.TB) (*dynamodb.Client, func()) {
	t.Helper()

	var dynamoClient *dynamodb.Client
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	runOpt := &dockertest.RunOptions{
		Repository: dynamoDBLocalRepo,
		Tag:        dynamoDBLocalTag,

		PortBindings: map[docker.Port][]docker.PortBinding{
			"0/tcp": {{HostIP: "localhost", HostPort: "8000/tcp"}},
		},
	}
	resource, err := pool.RunWithOptions(runOpt)
	if err != nil {
		t.Fatalf("Could not start DynamoDB Local: %s", err)
	}

	t.Logf("Using host:port of '%s'", resource.GetHostPort("8000/tcp"))

	if err = pool.Retry(func() error {
		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(
				aws.EndpointResolverWithOptionsFunc(
					func(service, region string, options ...interface{}) (aws.Endpoint, error) {
						return aws.Endpoint{URL: "http://" + resource.GetHostPort("8000/tcp")}, nil
					})),
			config.WithCredentialsProvider(
				credentials.StaticCredentialsProvider{
					Value: aws.Credentials{
						AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
						Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
					},
				}),
		)
		if err != nil {
			return err
		}

		dynamoClient = dynamodb.NewFromConfig(cfg)
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to the Docker instance of DynamoDB Local: %s", err)
	}

	return dynamoClient, func() {
		if err = pool.Purge(resource); err != nil {
			t.Fatalf("Could not purge DynamoDB: %s", err)
		}
	}
}

type InitialTableSetup struct {
	Table       *dynamodb.CreateTableInput
	InitialData []*types.PutRequest
}

// PrepTable iterates the provided input, creates tables and put items.
func PrepTable(t testing.TB, client *dynamodb.Client, input ...InitialTableSetup) {
	t.Helper()

	// Add extra retry setup in case Docker instance is busy. This can happen
	// especially within a CI environment, and the default retry count of 3
	// times is too fragile.
	opt := func(o *dynamodb.Options) { o.RetryMaxAttempts = 10 }
	for _, i := range input {
		_, err := client.CreateTable(context.Background(), i.Table, opt)
		if err != nil {
			t.Fatalf("Could not create table '%s': %v", *i.Table.TableName, err)
		}

		puts := []types.WriteRequest{}
		for _, d := range i.InitialData {
			puts = append(puts, types.WriteRequest{PutRequest: d})
		}
		_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*i.Table.TableName: puts,
			},
		})
		if err != nil {
			t.Fatalf("Could not write data to table '%s': %v", *i.Table.TableName, err)
		}

		t.Logf("Table '%s' has been created", *i.Table.TableName)
	}
}
