package dynamotest_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"

	"github.com/upsidr/dynamotest"
)

func TestSimple(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		initialSetup []dynamotest.InitialTableSetup
		query        *dynamodb.QueryInput
	}{
		"simple table with single data": {
			initialSetup: []dynamotest.InitialTableSetup{
				{
					Table: &dynamodb.CreateTableInput{
						AttributeDefinitions: []types.AttributeDefinition{
							{
								AttributeName: aws.String("id"),
								AttributeType: types.ScalarAttributeTypeS,
							},
						},
						KeySchema: []types.KeySchemaElement{
							{
								AttributeName: aws.String("id"),
								KeyType:       types.KeyTypeHash,
							},
						},
						TableName:   aws.String("my-table"),
						BillingMode: types.BillingModePayPerRequest,
					},
					InitialData: []*types.PutRequest{
						{
							Item: map[string]types.AttributeValue{
								"id":    &types.AttributeValueMemberS{Value: "123"},
								"name":  &types.AttributeValueMemberS{Value: "John Doe"},
								"email": &types.AttributeValueMemberS{Value: "john@doe.io"},
							},
						},
					},
				},
			},
			query: &dynamodb.QueryInput{
				TableName:              aws.String("my-table"),
				KeyConditionExpression: aws.String("id = :hashKey"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":hashKey": &types.AttributeValueMemberS{Value: "123"},
				},
			},
		},
		"sortable table": {
			initialSetup: []dynamotest.InitialTableSetup{
				{
					Table: &dynamodb.CreateTableInput{
						TableName: aws.String("sortable-table"),
						AttributeDefinitions: []types.AttributeDefinition{
							{
								AttributeName: aws.String("s_id"),
								AttributeType: types.ScalarAttributeTypeS,
							},
							{
								AttributeName: aws.String("date"),
								AttributeType: types.ScalarAttributeTypeS,
							},
						},
						KeySchema: []types.KeySchemaElement{
							{
								AttributeName: aws.String("s_id"),
								KeyType:       types.KeyTypeHash,
							},
							{
								AttributeName: aws.String("date"),
								KeyType:       types.KeyTypeRange,
							},
						},
						BillingMode: types.BillingModePayPerRequest,
					},
					InitialData: []*types.PutRequest{
						{
							Item: map[string]types.AttributeValue{
								"s_id": &types.AttributeValueMemberS{Value: "111"},
								"date": &types.AttributeValueMemberS{Value: "2022-02-15"},
							},
						},
						{
							Item: map[string]types.AttributeValue{
								"s_id": &types.AttributeValueMemberS{Value: "111"},
								"date": &types.AttributeValueMemberS{Value: "2022-02-16"},
							},
						},
						{
							Item: map[string]types.AttributeValue{
								"s_id": &types.AttributeValueMemberS{Value: "111"},
								"date": &types.AttributeValueMemberS{Value: "2022-02-17"},
							},
						},
					},
				},
			},
			query: &dynamodb.QueryInput{
				TableName:              aws.String("sortable-table"),
				ScanIndexForward:       aws.Bool(false), // Descending
				Limit:                  aws.Int32(5),
				KeyConditionExpression: aws.String("s_id = :hashKey and #date > :sortKey"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":hashKey": &types.AttributeValueMemberS{Value: "111"},
					":sortKey": &types.AttributeValueMemberS{Value: "2022-02-15"}, // IMPORTANT, filetring first item
				},
				ExpressionAttributeNames: map[string]string{
					"#date": "date",
				},
			},
		},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// dynamotest can be safely used in parallel testing.
			t.Parallel()

			client, clean := dynamotest.NewDynamoDB(t)
			defer clean()

			// Data prep, use simple context.
			dynamotest.PrepTable(t, client, tc.initialSetup...)

			// If no query defined, return early.
			if tc.query == nil {
				return
			}

			out, err := client.Query(context.Background(), tc.query)
			if err != nil {
				t.Errorf("failed to query, %v", err)
			}

			for idx, data := range out.Items {
				t.Logf("Data for %d\n", idx)

				for key, attr := range data {
					t.Logf("  Working on %s\n", key)
					t.Logf("  %v", attr)
				}
			}

			// t.Errorf("complete") // Comment this in to view the above logs.
		})
	}
}

type testData struct {
	PK string `dynamodbav:"test_PK" json:"test_PK"`

	X string `dynamodbav:"X" json:"X"`
	Y string `dynamodbav:"Y" json:"Y"`
	Z string `dynamodbav:"Z" json:"Z"`
}

func TestQueryWithUnmarshal(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		initialSetup []dynamotest.InitialTableSetup
		query        *dynamodb.QueryInput
		want         interface{}
	}{
		"simple table with query to unmarshall": {
			initialSetup: []dynamotest.InitialTableSetup{
				{
					Table: &dynamodb.CreateTableInput{
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
						TableName:   aws.String("test-table"),
						BillingMode: types.BillingModePayPerRequest,
					},
					InitialData: []*types.PutRequest{
						{
							Item: map[string]types.AttributeValue{
								"test_PK": &types.AttributeValueMemberS{Value: "XYZ"},
								"X":       &types.AttributeValueMemberS{Value: "Data for X"},
								"Y":       &types.AttributeValueMemberS{Value: "Data for Y"},
								"Z":       &types.AttributeValueMemberS{Value: "Data for Z"},
							},
						},
					},
				},
			},
			query: &dynamodb.QueryInput{
				TableName:              aws.String("test-table"),
				KeyConditionExpression: aws.String("test_PK = :hashKey"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":hashKey": &types.AttributeValueMemberS{Value: "XYZ"},
				},
			},
			want: &testData{
				PK: "XYZ",
				X:  "Data for X",
				Y:  "Data for Y",
				Z:  "Data for Z",
			},
		},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// dynamotest can be safely used in parallel testing.
			t.Parallel()

			client, clean := dynamotest.NewDynamoDB(t)
			defer clean()

			// Data prep, use simple context.
			dynamotest.PrepTable(t, client, tc.initialSetup...)

			// If no query defined, return early.
			if tc.query == nil {
				return
			}

			out, err := client.Query(context.Background(), tc.query)
			if err != nil {
				t.Errorf("failed to query, %v", err)
			}

			var got *testData = &testData{}
			err = attributevalue.UnmarshalMap(out.Items[0], got)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(testData{})); diff != "" {
				t.Errorf("received data didn't match (-want / +got)\n%s", diff)
			}
		})
	}
}
