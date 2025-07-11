import { Client } from '@opensearch-project/opensearch';
import * as fs from 'fs';
import * as path from 'path';

// Load environment variables from .env file
function loadEnv() {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) {
    console.error('Error: .env file not found. Please run setup_opensearch_model.sh first.');
    process.exit(1);
  }
  
  const envContent = fs.readFileSync(envPath, 'utf8');
  const envVars: { [key: string]: string } = {};
  
  envContent.split('\n').forEach(line => {
    const [key, value] = line.split('=');
    if (key && value) {
      envVars[key.trim()] = value.trim();
    }
  });
  
  return envVars;
}

const env = loadEnv();
const MODEL_ID = env.MODEL_ID;

if (!MODEL_ID) {
  console.error('Error: MODEL_ID not found in .env file. Please run setup_opensearch_model.sh first.');
  process.exit(1);
}

// Configure the OpenSearch client
const client = new Client({
  node: process.env.OPENSEARCH_URL || 'http://localhost:9200',
  requestTimeout: 60000,
  pingTimeout: 3000,
});

interface SearchResult {
  _id: string;
  _score: number;
  _source: {
    full_text: string;
    username: string;
    created_at: string;
    retweet_count: string;
    favorite_count: string;
    tweet_id?: string;
  };
}

async function neuralSearch(queryText: string, size: number = 5, k: number = 10) {
  console.log(`🔍 Searching for: "${queryText}"`);
  console.log(`📊 Using model: ${MODEL_ID}`);
  console.log('⏳ Performing neural search...\n');

  try {
    const response = await client.search({
      index: 'posts',
      body: {
        size: size,
        query: {
          neural: {
            full_text_vector: {
              query_text: queryText,
              model_id: MODEL_ID,
              k: k
            }
          }
        },
        _source: ['full_text', 'username', 'created_at', 'retweet_count', 'favorite_count', 'tweet_id']
      }
    });

    const hits = response.body.hits.hits as SearchResult[];
    
    if (hits.length === 0) {
      console.log('❌ No results found');
      return;
    }

    console.log(`✅ Found ${hits.length} results:\n`);
    
    hits.forEach((hit, index) => {
      const source = hit._source;
      const score = hit._score.toFixed(3);
      const text = source.full_text.length > 150 
        ? source.full_text.substring(0, 150) + '...' 
        : source.full_text;
      
      console.log(`${index + 1}. Score: ${score}`);
      console.log(`   User: ${source.username}`);
      console.log(`   Date: ${new Date(source.created_at).toLocaleDateString()}`);
      console.log(`   Engagement: ${source.retweet_count} RTs, ${source.favorite_count} likes`);
      console.log(`   Text: ${text}`);
      console.log('');
    });

    console.log(`📈 Search took: ${response.body.took}ms`);
    
  } catch (error) {
    console.error('❌ Search error:', error);
    process.exit(1);
  }
}

// Neural search with date sorting (preserves scores)
async function neuralSearchSortedByDate(queryText: string, size: number = 10, k: number = 50) {
  console.log(`🔍 Searching for: "${queryText}"`);
  console.log(`📊 Using model: ${MODEL_ID}`);
  console.log('⏳ Performing neural search, then sorting by date...\n');

  try {
    // First, get results with scores (no sorting)
    const response = await client.search({
      index: 'posts',
      body: {
        size: k, // Get more results for better sorting
        query: {
          neural: {
            full_text_vector: {
              query_text: queryText,
              model_id: MODEL_ID,
              k: k
            }
          }
        },
        _source: ['full_text', 'username', 'created_at', 'retweet_count', 'favorite_count', 'tweet_id']
      }
    });

    const hits = response.body.hits.hits as SearchResult[];
    
    if (hits.length === 0) {
      console.log('❌ No results found');
      return;
    }

    // Sort by date in JavaScript (preserves scores)
    const sortedHits = hits.sort((a, b) => {
      return new Date(b._source.created_at).getTime() - new Date(a._source.created_at).getTime();
    });

    // Take only the requested number of results
    const topResults = sortedHits.slice(0, size);
    console.log(`✅ Found ${topResults.length} results (sorted by date, scores preserved):\n`);
    
    topResults.forEach((hit, index) => {
      const source = hit._source;
      const score = hit._score.toFixed(3);
      const text = source.full_text.length > 150 
        ? source.full_text.substring(0, 150) + '...' 
        : source.full_text;
      
      console.log(`${index + 1}. Score: ${score}`);
      console.log(`   User: ${source.username}`);
      console.log(`   Date: ${new Date(source.created_at).toLocaleDateString()}`);
      console.log(`   Engagement: ${source.retweet_count} RTs, ${source.favorite_count} likes`);
      console.log(`   Text: ${text}`);
      console.log('');
    });

    console.log(`📈 Search took: ${response.body.took}ms`);
    
  } catch (error) {
    console.error('❌ Search error:', error);
    process.exit(1);
  }
}

// Handle hybrid search (neural + keyword)
async function hybridSearch(queryText: string, size: number = 5, k: number = 10) {
  console.log(`🔍 Hybrid search for: "${queryText}"`);
  console.log(`📊 Using model: ${MODEL_ID}`);
  console.log('⏳ Performing hybrid search (neural + keyword)...\n');

  try {
    const response = await client.search({
      index: 'posts',
      body: {
        size: size,
        query: {
          hybrid: {
            queries: [
              {
                neural: {
                  full_text_vector: {
                    query_text: queryText,
                    model_id: MODEL_ID,
                    k: k
                  }
                }
              },
              {
                match: {
                  full_text: {
                    query: queryText,
                    boost: 0.3
                  }
                }
              }
            ]
          }
        },
        _source: ['full_text', 'username', 'created_at', 'retweet_count', 'favorite_count', 'tweet_id']
      }
    });

    const hits = response.body.hits.hits as SearchResult[];
    
    if (hits.length === 0) {
      console.log('❌ No results found');
      return;
    }

    console.log(`✅ Found ${hits.length} results:\n`);
    
    hits.forEach((hit, index) => {
      const source = hit._source;
      const score = hit._score.toFixed(3);
      const text = source.full_text.length > 150 
        ? source.full_text.substring(0, 150) + '...' 
        : source.full_text;
      
      console.log(`${index + 1}. Score: ${score}`);
      console.log(`   User: ${source.username}`);
      console.log(`   Date: ${new Date(source.created_at).toLocaleDateString()}`);
      console.log(`   Engagement: ${source.retweet_count} RTs, ${source.favorite_count} likes`);
      console.log(`   Text: ${text}`);
      console.log('');
    });

    console.log(`📈 Search took: ${response.body.took}ms`);
    
  } catch (error) {
    console.error('❌ Hybrid search not supported, falling back to neural search');
    await neuralSearch(queryText, size, k);
  }
}

// Main function
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args.includes('--help')) {
    console.log('🔍 Neural Search for Posts');
    console.log('');
    console.log('Usage:');
    console.log('  bun neural_search.ts <query> [options]');
    console.log('');
    console.log('Options:');
    console.log('  --size <number>    Number of results to return (default: 5)');
    console.log('  --k <number>       Number of nearest neighbors to consider (default: 10)');
    console.log('  --hybrid           Use hybrid search (neural + keyword)');
    console.log('  --sort-by-date     Sort results by date (newest first)');
    console.log('  --help             Show this help message');
    console.log('');
    console.log('Examples:');
    console.log('  bun neural_search.ts "artificial intelligence"');
    console.log('  bun neural_search.ts "cryptocurrency bitcoin" --size 10');
    console.log('  bun neural_search.ts "climate change" --hybrid --k 20');
    console.log('  bun neural_search.ts "machine learning" --sort-by-date --size 10');
    console.log('');
    console.log('Model ID: ' + MODEL_ID);
    return;
  }

  const queryText = args[0];
  const sizeIndex = args.indexOf('--size');
  const kIndex = args.indexOf('--k');
  const isHybrid = args.includes('--hybrid');
  const sortByDate = args.includes('--sort-by-date');
  
  const size = sizeIndex !== -1 ? parseInt(args[sizeIndex + 1]) || 5 : 5;
  const k = kIndex !== -1 ? parseInt(args[kIndex + 1]) || 10 : 10;

  if (sortByDate) {
    await neuralSearchSortedByDate(queryText, size, k);
  } else if (isHybrid) {
    await hybridSearch(queryText, size, k);
  } else {
    await neuralSearch(queryText, size, k);
  }
}

main().catch(console.error);