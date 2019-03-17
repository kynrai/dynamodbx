package dynamodbx

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// CreateTableSync will create a dynamodb table and block until the table is created.
// This is useful in code which immediatly writes to a newly created table or for tests
func CreateTableSync(client *dynamodb.DynamoDB, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	out, err := client.CreateTable(input)
	if err != nil {
		return nil, err
	}

	for {
		out, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: input.TableName})
		if err != nil {
			return nil, err
		}
		if *out.Table.TableStatus == dynamodb.TableStatusActive {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	return out, nil
}

// CreateTableSyncWithContext will create a dynamodb table and block until the table is created.
// This is useful in code which immediatly writes to a newly created table or for tests
func CreateTableSyncWithContext(ctx context.Context, client *dynamodb.DynamoDB, input *dynamodb.CreateTableInput, opts ...request.Option) (*dynamodb.CreateTableOutput, error) {
	out, err := client.CreateTableWithContext(ctx, input, opts...)
	if err != nil {
		return nil, err
	}

	for {
		out, err := client.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{TableName: input.TableName}, opts...)
		if err != nil {
			return nil, err
		}
		if *out.Table.TableStatus == dynamodb.TableStatusActive {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	return out, nil
}
