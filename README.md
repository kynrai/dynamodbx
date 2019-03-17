# DynamoDBx

Extra functions to help wtih quality of life when using dynamodb

## Roadmap

Requests for feature priority welcome, and PRs most welcome

- [ ] BatchWrite
- [ ] BatchUpdate

## Key Features

### Auto-handling batch operations

Many batch operations with the aws-sdk-go expect you to handle batching and retries of failed operations manually. While in some languages such as boto3 this is handled for you, we can attempt the same thing in go.

### `BatchWrite`

Create a batch of items from a slice

```go
type Stuff struct {
    Foo string
    Bar int
}

tablename := "test_table"

input := []*Stuff{
    {"Hello", 1},
    {"World", 2}
}

req, err := dynamodbx.BatchPutRequest(tableName, input)
if err != nil {
    return err
}
```

This returns a `map[string][]*dynamodb.WriteRequest` which can be used in the `dynamodbx.BatchWriteItem` function. This can handle any number of items that will fit int he slice instead of the usual 25.

```go
ddb := dynamodb.New(
session.Must(session.NewSession(
    &aws.Config{
        Region:   aws.String("eu-west-1"),
        Endpoint: aws.String("http://localhost:8000"),
        },
    )),
)

out, err := dynamodbx.BatchWriteItem(ddb, &dynamodb.BatchWriteItemInput{
    ReturnConsumedCapacity:      aws.String("TOTAL"),
    ReturnItemCollectionMetrics: aws.String("SIZE"),
    RequestItems:                req,
})
```

The inputs and outputs are the same as the default aws-sdk-go but with aggregated metrics for each table

### synchronous Operations

There are number of operations which are async, such as create table. There are often times where we need to keep polling DynamoDB to wait for a status before we can proceed.

We can ease this process by creating a few helper funtions where it makes sense.

### `CreateTable`

`CreateTableSync` and `CreateTableSyncWithContext` wrap the existing create table function in aws-adk-go and just implement the polling of describe table thus blocking until the table is created.

```go
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
```

This function should only return when the table is in a ready state
