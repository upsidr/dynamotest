package dynamotest_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/upsidr/dynamotest"
)

func TestMinimalSetupEmpty(t *testing.T) {
	t.Skip("As this is only to demonstrate the minimal setup, this actually " +
		"fails with required fields missing for creating tables.")

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

func TestMinimalSetup(t *testing.T) {
	// Because dynamotest is designed to create a separate DynamoDB Local
	// instance, it is safe to run all the tests in parallel.
	t.Parallel()

	// Create a new DynamoDB Local instance. Second return value can be called
	// to delete the instance.
	client, clean := dynamotest.NewDynamoDB(t)
	defer clean()

	// Prepare table with some data.
	dynamotest.PrepTable(t, client,
		dynamotest.InitialTableSetup{
			Table: &dynamodb.CreateTableInput{
				TableName:   aws.String("test-table"),
				BillingMode: types.BillingModePayPerRequest,
				AttributeDefinitions: []types.AttributeDefinition{
					{
						AttributeName: aws.String("test_PK"),
						AttributeType: types.ScalarAttributeTypeS,
					},
				},
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("test_PK"),
						KeyType:       types.KeyTypeHash,
					},
				},
			},
			InitialData: []*types.PutRequest{
				{
					Item: map[string]types.AttributeValue{
						"test_PK": &types.AttributeValueMemberS{Value: "XYZ"},
						"X":       &types.AttributeValueMemberS{Value: "Data for X"},
						"Y":       &types.AttributeValueMemberS{Value: "Data for Y"},
						"Z":       &types.AttributeValueMemberS{Value: "Data for Z"},
						"Time":    &types.AttributeValueMemberS{Value: "2022-04-28T13:45:01Z"},
					},
				},
			},
		},
		// Provide as many initial data as you need
	)

	// Get data by a simple BatchGetItem call against the table created above.
	data, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"test-table": {
				Keys: []map[string]types.AttributeValue{
					{
						"test_PK": &types.AttributeValueMemberS{Value: "XYZ"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Prepare unmarshal setup.
	type testdata struct {
		PK string `dynamodbav:"test_PK" json:"test_PK"`

		X string `dynamodbav:"X" json:"X"`
		Y string `dynamodbav:"Y" json:"Y"`
		Z string `dynamodbav:"Z" json:"Z"`

		Time time.Time `dynamodbav:"Time" json:"Time"`
	}

	// Unmarshal the returned data.
	got := &testdata{}
	err = attributevalue.UnmarshalMapWithOptions(data.Responses["test-table"][0], got)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare timestamp parse function for demo only.
	parse := func(s string) time.Time {
		x, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	// Prepare the data to check against.
	want := &testdata{
		PK:   "XYZ",
		X:    "Data for X",
		Y:    "Data for Y",
		Z:    "Data for Z",
		Time: parse("2022-04-28T13:45:01Z"),
	}

	// Confirm that the DynamoDB returned the data as expected.
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("data didn't match (-want / +got)\n%s", diff)
	}
}
