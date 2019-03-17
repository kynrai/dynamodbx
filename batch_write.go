package dynamodbx

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// BatchWriteItem is a wrapper around the aws-sdk-go dynamodb.BatchWriteItem. It will attemptt to
// automatically breakup batch writes into smaller batches so that inputs larger tthan 25 items
// can be easily processed. Use it as a drop in replacement for the existting BatchWriteItem command
// but with the dynamodb client supplied as the first paramter.
func BatchWriteItem(client *dynamodb.DynamoDB, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	fOut := &dynamodb.BatchWriteItemOutput{}
	for tableName, items := range input.RequestItems {
		var unprocessedItems map[string][]*dynamodb.WriteRequest
		chunkSize := 25
		for i := 0; i < len(items); i += chunkSize {
			end := i + chunkSize
			if end > len(items) {
				end = len(items)
			}
			batch := &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
				ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
				RequestItems: map[string][]*dynamodb.WriteRequest{
					tableName: items[i:end],
				},
			}
			if len(unprocessedItems) > 0 {
				for k, v := range unprocessedItems {
					batch.RequestItems[k] = append(batch.RequestItems[k], v...)
				}
			}
			// after adding all the unprocessed items we reset the chunkSize back to 25
			chunkSize = 25

			out, err := client.BatchWriteItem(batch)
			if err != nil {
				return nil, err
			}
			fOut.ConsumedCapacity = append(fOut.ConsumedCapacity, out.ConsumedCapacity...)
			for k, v := range out.ItemCollectionMetrics {
				fOut.ItemCollectionMetrics[k] = append(fOut.ItemCollectionMetrics[k], v...)
			}

			// If we have no unprocessed items we make sure the next iteration has the full chunkSize
			if len(out.UnprocessedItems) == 0 {
				chunkSize = 25
				continue
			}

			// We have unprocessed items at this point, we deduct the number from the chunkSize to add
			// them both next iteration
			chunkSize -= len(out.UnprocessedItems)
		}
	}
	// Sum up multiple ConsumedCapacity structs
	sum := make(map[string]*dynamodb.ConsumedCapacity)
	for _, v := range fOut.ConsumedCapacity {
		if _, ok := sum[*v.TableName]; !ok {
			sum[*v.TableName] = &dynamodb.ConsumedCapacity{
				CapacityUnits: v.CapacityUnits,
			}
			continue
		}
		*sum[*v.TableName].CapacityUnits += *v.CapacityUnits
	}
	sliceCap := make([]*dynamodb.ConsumedCapacity, 0, len(sum))
	for k, v := range sum {
		sliceCap = append(sliceCap, &dynamodb.ConsumedCapacity{
			TableName:     aws.String(k),
			CapacityUnits: v.CapacityUnits,
		})
	}
	fOut.ConsumedCapacity = sliceCap
	return fOut, nil
}

// BatchWriteItemWithContext is a wrapper around the aws-sdk-go dynamodb.BatchWriteItemWithContext. It will attemptt to
// automatically breakup batch writes into smaller batches so that inputs larger tthan 25 items
// can be easily processed. Use it as a drop in replacement for the existting BatchWriteItem command
// but with the dynamodb client supplied as the first paramter.
// A context and request options can be provided to and will be passed to the underlying aws-sdk-go calls
func BatchWriteItemWithContext(ctx context.Context, client *dynamodb.DynamoDB, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	fOut := &dynamodb.BatchWriteItemOutput{}
	for tableName, items := range input.RequestItems {
		var unprocessedItems map[string][]*dynamodb.WriteRequest
		chunkSize := 25
		for i := 0; i < len(items); i += chunkSize {
			end := i + chunkSize
			if end > len(items) {
				end = len(items)
			}
			batch := &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
				ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
				RequestItems: map[string][]*dynamodb.WriteRequest{
					tableName: items[i:end],
				},
			}
			if len(unprocessedItems) > 0 {
				for k, v := range unprocessedItems {
					batch.RequestItems[k] = append(batch.RequestItems[k], v...)
				}
			}
			// after adding all the unprocessed items we reset the chunkSize back to 25
			chunkSize = 25

			out, err := client.BatchWriteItemWithContext(ctx, batch, opts...)
			if err != nil {
				return nil, err
			}
			fOut.ConsumedCapacity = append(fOut.ConsumedCapacity, out.ConsumedCapacity...)
			for k, v := range out.ItemCollectionMetrics {
				fOut.ItemCollectionMetrics[k] = append(fOut.ItemCollectionMetrics[k], v...)
			}

			// If we have no unprocessed items we make sure the next iteration has the full chunkSize
			if len(out.UnprocessedItems) == 0 {
				chunkSize = 25
				continue
			}

			// We have unprocessed items at this point, we deduct the number from the chunkSize to add
			// them both next iteration
			chunkSize -= len(out.UnprocessedItems)
		}
	}
	// Sum up multiple ConsumedCapacity structs
	sum := make(map[string]*dynamodb.ConsumedCapacity)
	for _, v := range fOut.ConsumedCapacity {
		if _, ok := sum[*v.TableName]; !ok {
			sum[*v.TableName] = &dynamodb.ConsumedCapacity{
				CapacityUnits: v.CapacityUnits,
			}
			continue
		}
		*sum[*v.TableName].CapacityUnits += *v.CapacityUnits
	}
	sliceCap := make([]*dynamodb.ConsumedCapacity, 0, len(sum))
	for k, v := range sum {
		sliceCap = append(sliceCap, &dynamodb.ConsumedCapacity{
			TableName:     aws.String(k),
			CapacityUnits: v.CapacityUnits,
		})
	}
	fOut.ConsumedCapacity = sliceCap
	return fOut, nil
}
