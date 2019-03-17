package dynamodbx

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	ErrEmptyTableName = errors.New("dynamodbx/BatchPutRequest: table name cannot be empty")
	ErrInterfaceNil   = errors.New("dynamodbx/BatchPutRequest: interface cannot be nil")
	ErrInterfaceSlice = errors.New("dynamodbx/BatchPutRequest: the interface is not a slice")
)

// BatchPutRequest creates a dynamodb WriteRequest batch for use with requests which require
// BatchWriteItem. This is mainly used as a helper method to convert go structs to a dynamodb
// PutRequest.
//
// IMPORTANT: The resulting map[string][]*dynamodb.WriteRequest can be over 25 items which is over the
// AWS limit for a batch write. The result of this fumction is intended to be used as input to the dynamodbx
// Batch operations which will handle much larger batches by splitting the items into multiple requests.
//
// The input tablename must be the name of the dynamodb table the PutRequest will be used agianst.
// Tablename cannot be empty.
//
// The input v must be a slice of golang structs which can be converted to dynamodb attrivuted
// using the dynamodbattribute.MarshalMap function. v Cannot be nil.
func BatchPutRequest(table string, v interface{}) (map[string][]*dynamodb.WriteRequest, error) {
	if table == "" {
		return nil, ErrEmptyTableName
	}
	if v == nil {
		return nil, ErrInterfaceNil
	}
	if reflect.TypeOf(v).Kind() != reflect.Slice {
		return nil, ErrInterfaceSlice
	}
	items := reflect.ValueOf(v)
	reqs := make([]*dynamodb.WriteRequest, 0, items.Len())

	for i := 0; i < items.Len(); i++ {
		data, err := dynamodbattribute.MarshalMap(items.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		req := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: data},
		}
		reqs = append(reqs, req)
	}
	return map[string][]*dynamodb.WriteRequest{table: reqs}, nil
}
