import { createReadStream } from 'fs';
import { parse } from 'csv-parse';
import { Client } from '@opensearch-project/opensearch';
  
const BATCH_SIZE = 1000;
const INDEX_NAME = 'posts';

// Configure the OpenSearch client
// Connects to the 'opensearch' service from docker-compose.yml
const client = new Client({
  node: process.env.OPENSEARCH_URL || 'http://localhost:9200',
});

async function ingestCSV(filePath: string) {
  console.log(`Starting ingestion of ${filePath} into index "${INDEX_NAME}"...`);
  
  const parser = createReadStream(filePath).pipe(
    parse({
      columns: true, // Use the first row as headers
      trim: true,
    })
  );

  let batch: any[] = [];
  let rowCount = 0;

  // Check if the index exists, and create it if it doesn't
  const { body: indexExists } = await client.indices.exists({ index: INDEX_NAME });
  if (!indexExists) {
      console.log(`Index "${INDEX_NAME}" does not exist. Creating...`);
      await client.indices.create({
          index: INDEX_NAME,
          body: {
              mappings: {
                  properties: {
                      tweet_id: { type: 'keyword' },
                      account_id: { type: 'keyword' },
                      created_at: { type: 'date' }, // Use default date mapping
                      full_text: { type: 'text' },
	                  retweet_count: { type: 'integer' },
	                  favorite_count: { type: 'integer' },
	                  reply_to_tweet_id: { type: 'keyword' },
	                  reply_to_user_id: { type: 'keyword' },
	                  reply_to_username: { type: 'keyword' },
	                  username: { type: 'keyword' },
                  },
              },
          },
      });
      console.log(`Index "${INDEX_NAME}" created.`);
  } else {
      console.log(`Index "${INDEX_NAME}" already exists.`);
  }

  for await (const record of parser) {
    // Convert created_at to ISO 8601 format before indexing
    if (record.created_at) {
      try {
        // The Date constructor can parse 'YYYY-MM-DD HH:MM:SS+ZZ:ZZ'
        record.created_at = new Date(record.created_at).toISOString();
      } catch (e) {
        console.error(`Could not parse date: ${record.created_at}`);
        // Set to null if parsing fails. OpenSearch will reject if the field is not nullable.
        record.created_at = null;
      }
    }

    batch.push({ index: { _index: INDEX_NAME } });
    batch.push(record);
    rowCount++;

    if (batch.length >= BATCH_SIZE * 2) {
      await sendBulkRequest(batch);
      console.log(`Ingested ${rowCount} rows...`);
      batch = [];
    }
  }

  // Ingest any remaining documents
  if (batch.length > 0) {
    await sendBulkRequest(batch);
  }

  console.log(`Finished ingestion.`);
  console.log(`Total rows processed: ${rowCount}`);

  // Refresh the index to make the documents searchable
  await client.indices.refresh({ index: INDEX_NAME });
  const countResponse = await client.count({ index: INDEX_NAME });
  console.log(`Total documents in index "${INDEX_NAME}": ${countResponse.body.count}`);
}

async function sendBulkRequest(batch: any[]) {
  try {
    const response = await client.bulk({
      body: batch,
    });

    if (response.body.errors) {
      console.error('Bulk ingestion had errors:');
      // Log detailed information for failed items
      response.body.items.forEach((item: any) => {
        if (item.index && item.index.error) {
          console.error(`Error for item ${item.index._id}:`, item.index.error);
        }
      });
    }
  } catch (error) {
    console.error('Error during bulk ingestion:', error);
    // It might be useful to exit or implement a retry mechanism here
    process.exit(1);
  }
}

// --- Script execution ---
async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Please provide the path to the CSV file.');
    console.error('Usage: bun ingest_csv.ts <path-to-csv-file>');
    process.exit(1);
  }

  try {
    await ingestCSV(filePath);
  } catch (error) {
    console.error('An unexpected error occurred:', error);
    process.exit(1);
  }
}

main();