package dynamodbx_test

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/kylelemons/godebug/pretty"
	"github.com/kynrai/dynamodbx"
)

// TODO: Find a way to to test unprocessed items
func TestBatchWriteItem(t *testing.T) {
	t.Parallel()
	type TestData struct {
		S string
	}
	testDataSet := func(count int) []*TestData {
		data := make([]*TestData, count)
		for i := 0; i < count; i++ {
			data[i] = &TestData{
				S: strconv.Itoa(i),
			}
		}
		return data
	}
	for _, tc := range []struct {
		name   string
		table  string
		input  interface{}
		expect *dynamodb.BatchWriteItemOutput
	}{
		{
			name:  "insert 2 items",
			table: "test1",
			input: testDataSet(2),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(2),
						TableName:     aws.String("test1"),
					},
				},
			},
		},
		{
			name:  "insert 115 items",
			table: "testSet",
			input: testDataSet(115),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(115),
						TableName:     aws.String("testSet"),
					},
				},
			},
		},
		{
			name:  "insert 1000 items",
			table: "testLargeSet",
			input: testDataSet(1000),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(1000),
						TableName:     aws.String("testLargeSet"),
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Must have a local dynamodb running
			ddb := dynamodb.New(
				session.Must(session.NewSession(
					&aws.Config{
						Region:      aws.String("eu-west-1"),
						Endpoint:    aws.String("http://localhost:8000"),
						Credentials: credentials.NewStaticCredentials("foo", "bar", "foobar"),
					},
				)),
			)
			_, err := dynamodbx.CreateTableSync(ddb, &dynamodb.CreateTableInput{
				TableName:   aws.String(tc.table),
				BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("S"),
						AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("S"),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
				},
			})
			defer ddb.DeleteTable(&dynamodb.DeleteTableInput{
				TableName: aws.String(tc.table),
			})
			if err != nil {
				t.Fatal(err)
			}
			req, err := dynamodbx.BatchPutRequest(tc.table, tc.input)
			if err != nil {
				t.Fatal(err)
			}
			out, err := dynamodbx.BatchWriteItem(ddb, &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity:      aws.String("TOTAL"),
				ReturnItemCollectionMetrics: aws.String("SIZE"),
				RequestItems:                req,
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.expect != nil && !reflect.DeepEqual(out, tc.expect) {
				t.Fatalf(pretty.Compare(out, tc.expect))
			}
		})
	}
}

func TestBatchWriteItemWithContext(t *testing.T) {
	t.Parallel()
	type TestData struct {
		S string
	}
	testDataSet := func(count int) []*TestData {
		data := make([]*TestData, count)
		for i := 0; i < count; i++ {
			data[i] = &TestData{
				S: strconv.Itoa(i),
			}
		}
		return data
	}
	ctxDead, cfunc := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute*1))
	defer cfunc()
	for _, tc := range []struct {
		ctx     context.Context
		name    string
		table   string
		input   interface{}
		expect  *dynamodb.BatchWriteItemOutput
		errText string
	}{
		{
			name:  "insert 2 items",
			table: "test1",
			input: testDataSet(2),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(2),
						TableName:     aws.String("test1"),
					},
				},
			},
			ctx: context.Background(),
		},
		{
			name:  "insert 115 items",
			table: "testSet",
			input: testDataSet(115),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(115),
						TableName:     aws.String("testSet"),
					},
				},
			},
			ctx: context.Background(),
		},
		{
			name:  "insert 1000 items",
			table: "testLargeSet",
			input: testDataSet(1000),
			expect: &dynamodb.BatchWriteItemOutput{
				ConsumedCapacity: []*dynamodb.ConsumedCapacity{
					{
						CapacityUnits: aws.Float64(1000),
						TableName:     aws.String("testLargeSet"),
					},
				},
			},
			ctx: context.Background(),
		},
		{
			name:    "context timeout",
			table:   "testLargeSetWithCtx",
			input:   testDataSet(1000),
			ctx:     ctxDead,
			errText: "context deadline exceeded",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Must have a local dynamodb running
			ddb := dynamodb.New(
				session.Must(session.NewSession(
					&aws.Config{
						Region:      aws.String("eu-west-1"),
						Endpoint:    aws.String("http://localhost:8000"),
						Credentials: credentials.NewStaticCredentials("foo", "bar", "foobar"),
					},
				)),
			)
			_, err := dynamodbx.CreateTableSyncWithContext(tc.ctx, ddb, &dynamodb.CreateTableInput{
				TableName:   aws.String(tc.table),
				BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("S"),
						AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("S"),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
				},
			})
			defer ddb.DeleteTableWithContext(tc.ctx, &dynamodb.DeleteTableInput{
				TableName: aws.String(tc.table),
			})
			if tc.errText == "" && err != nil {
				t.Fatal(err)
			}
			if tc.errText != "" && !strings.Contains(err.Error(), tc.errText) {
				t.Fatalf("expected error %v got %v", tc.errText, err.Error())
			}
			req, err := dynamodbx.BatchPutRequest(tc.table, tc.input)
			if err != nil {
				t.Fatal(err)
			}
			out, err := dynamodbx.BatchWriteItemWithContext(tc.ctx, ddb, &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity:      aws.String("TOTAL"),
				ReturnItemCollectionMetrics: aws.String("SIZE"),
				RequestItems:                req,
			})
			if tc.errText == "" && err != nil {
				t.Fatal(err)
			}
			if tc.expect != nil && !reflect.DeepEqual(out, tc.expect) {
				t.Fatalf(pretty.Compare(out, tc.expect))
			}
		})
	}
}
