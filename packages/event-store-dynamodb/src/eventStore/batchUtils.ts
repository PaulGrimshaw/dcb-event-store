import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb"

export async function markBatchFailed(
    client: DynamoDBDocumentClient,
    tableName: string,
    batchId: string
): Promise<void> {
    try {
        await client.send(
            new UpdateCommand({
                TableName: tableName,
                Key: { PK: `_BATCH#${batchId}`, SK: `_BATCH#${batchId}` },
                UpdateExpression: "SET #s = :failed",
                ConditionExpression: "#s = :pending",
                ExpressionAttributeNames: { "#s": "status" },
                ExpressionAttributeValues: { ":failed": "FAILED", ":pending": "PENDING" }
            })
        )
    } catch (e: unknown) {
        if ((e as { name?: string }).name !== "ConditionalCheckFailedException") throw e
        // Already COMMITTED or FAILED — fine
    }
}
