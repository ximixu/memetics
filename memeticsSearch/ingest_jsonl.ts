import { createReadStream } from 'fs';
import * as readline from 'readline';
import { Client } from '@opensearch-project/opensearch';

const BATCH_SIZE = 100; // Reduced batch size for large vectors
const INDEX_NAME = 'posts';

// Configure the OpenSearch client with verbose logging
// Connects to the 'opensearch' service from docker-compose.yml
const client = new Client({
  node: process.env.OPENSEARCH_URL || 'http://localhost:9200',
  requestTimeout: 60000, // 60 second timeout
  pingTimeout: 3000,
  maxRetries: 3,
  log: 'trace', // Enable verbose logging
});

async function ingestJSONL(filePath: string) {
  console.log(`Starting ingestion of ${filePath} into index "${INDEX_NAME}"...`);
  console.log(`OpenSearch URL: ${process.env.OPENSEARCH_URL || 'http://localhost:9200'}`);
  
  // Test connection first
  try {
    const health = await client.cluster.health();
    console.log('OpenSearch cluster health:', JSON.stringify(health.body, null, 2));
  } catch (error) {
    console.error('Failed to connect to OpenSearch:', error);
    throw error;
  }

  const fileStream = createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let batch: any[] = [];
  let rowCount = 0;

  // Check if the index exists, and create it if it doesn't
  console.log(`Checking if index "${INDEX_NAME}" exists...`);
  const { body: indexExists } = await client.indices.exists({ index: INDEX_NAME });
  console.log(`Index exists: ${indexExists}`);
  if (!indexExists) {
    console.log(`Index "${INDEX_NAME}" does not exist. Creating...`);
    try {
      const createResponse = await client.indices.create({
        index: INDEX_NAME,
        body: {
          settings: {
              "index.knn": true,
          },
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
              full_text_vector: {
                type: 'knn_vector',
                dimension: 384, // From the sentence-transformers model
              },
            },
          },
        },
      });
      console.log('Index creation response:', JSON.stringify(createResponse.body, null, 2));
    } catch (error) {
      console.error('Failed to create index:', error);
      throw error;
    }
    console.log(`Index "${INDEX_NAME}" created.`);
  } else {
    console.log(`Index "${INDEX_NAME}" already exists.`);
  }

  for await (const line of rl) {
    if (line.trim() === '') {
        continue;
    }
    let record;
    try {
      record = JSON.parse(line);
    } catch (parseError) {
      console.error(`Failed to parse JSON line ${rowCount + 1}:`, parseError);
      console.error('Problematic line:', line);
      continue;
    }

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

async function sendBulkRequest(batch: any[], retryCount = 0) {
  console.log(`Sending bulk request with ${batch.length / 2} documents (attempt ${retryCount + 1})...`);
  
  try {
    const response = await client.bulk({
      body: batch,
      timeout: '60s', // Set request timeout
    });

    console.log(`Bulk request completed. Response status: ${response.statusCode}`);
    console.log(`Took: ${response.body.took}ms, Errors: ${response.body.errors}`);

    if (response.body.errors) {
      console.error('Bulk ingestion had errors:');
      let errorCount = 0;
      // Log detailed information for failed items
      response.body.items.forEach((item: any, index: number) => {
        if (item.index && item.index.error) {
          errorCount++;
          console.error(`Error for item ${index} (ID: ${item.index._id}):`);
          console.error('  Status:', item.index.status);
          console.error('  Error:', JSON.stringify(item.index.error, null, 2));
          
          // Log the actual document that failed
          const docIndex = index * 2 + 1; // Documents are at odd indices in the batch
          if (docIndex < batch.length) {
            console.error('  Document:', JSON.stringify(batch[docIndex], null, 2));
          }
        }
      });
      console.error(`Total errors: ${errorCount}`);
      
      // If more than half the batch failed, throw an error
      if (errorCount > batch.length / 4) {
        throw new Error(`Too many errors in batch: ${errorCount} out of ${batch.length / 2} documents`);
      }
    }
  } catch (error) {
    console.error(`Error during bulk ingestion (attempt ${retryCount + 1}):`);
    console.error('Error name:', error.name);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    
    // Log additional details if it's a connection error
    if (error.meta) {
      console.error('Error meta:', JSON.stringify(error.meta, null, 2));
    }
    
    if (retryCount < 2) {
      console.log(`Retrying in 5 seconds...`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      return sendBulkRequest(batch, retryCount + 1);
    } else {
      console.error('Max retries exceeded. Exiting.');
      throw error;
    }
  }
}

// --- Script execution ---
async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Please provide the path to the JSONL file.');
    console.error('Usage: bun ingest_jsonl.ts <path-to-jsonl-file>');
    process.exit(1);
  }

  try {
    await ingestJSONL(filePath);
  } catch (error) {
    console.error('An unexpected error occurred:');
    console.error('Error name:', error.name);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    if (error.meta) {
      console.error('Error meta:', JSON.stringify(error.meta, null, 2));
    }
    process.exit(1);
  }
}

main();