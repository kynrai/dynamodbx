package dynamodbx_test

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/kylelemons/godebug/pretty"
	"github.com/kynrai/dynamodbx"
)

func TestBatchUtilsBatchPutRequest(t *testing.T) {
	t.Parallel()

	type Item struct {
		Foo string
		Bar int
	}

	for _, tc := range []struct {
		name    string
		table   string
		input   interface{}
		err     error
		errText string
		expect  map[string][]*dynamodb.WriteRequest
	}{
		{
			name: "empty table name",
			err:  dynamodbx.ErrEmptyTableName,
		},
		{
			name:  "nil input",
			table: "test",
			err:   dynamodbx.ErrInterfaceNil,
		},
		{
			name:  "non slice input",
			table: "test",
			input: struct{}{},
			err:   dynamodbx.ErrInterfaceSlice,
		},
		{
			name:  "marshal items",
			table: "test",
			input: []Item{{"a", 1}, {"b", 2}},
			expect: map[string][]*dynamodb.WriteRequest{
				"test": []*dynamodb.WriteRequest{
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"Foo": &dynamodb.AttributeValue{S: aws.String("a")},
								"Bar": &dynamodb.AttributeValue{N: aws.String("1")},
							},
						},
					},
					{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"Foo": &dynamodb.AttributeValue{S: aws.String("b")},
								"Bar": &dynamodb.AttributeValue{N: aws.String("2")},
							},
						},
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, err := dynamodbx.BatchPutRequest(tc.table, tc.input)
			if tc.err != nil && tc.err != err {
				t.Fatalf("expected error mismatch: got: %v, want: %v", err, tc.err)
			}
			if tc.errText != "" && err == nil {
				t.Fatal("expected an error but got nil")
			}
			if tc.errText != "" && tc.errText != err.Error() {
				t.Fatalf("expected error mismatch: got: %v, want: %v", err.Error(), tc.errText)
			}
			if tc.err == nil && tc.errText == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.expect != nil && !reflect.DeepEqual(resp, tc.expect) {
				t.Fatal(pretty.Compare(resp, tc.expect))
			}
		})
	}
}
