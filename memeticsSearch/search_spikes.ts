import { Client } from '@opensearch-project/opensearch';

const INDEX_NAME = 'posts';

// Configure the OpenSearch client
const client = new Client({
  node: process.env.OPENSEARCH_URL || 'http://localhost:9200',
});

async function searchSpikes() {
  console.log('Searching for spikes in word usage...');

  try {
    const response = await client.search({
      index: INDEX_NAME,
      body: {
        size: 0,
        aggs: {
          monthly_usage: {
            date_histogram: {
              field: 'created_at',
              calendar_interval: 'month',
              format: 'yyyy-MM',
            },
            aggs: {
              word_counts: {
                terms: {
                  field: 'full_text',
                  size: 1000, // Adjust size as needed to capture all relevant words
                },
              },
            },
          },
        },
      },
    });

    const buckets = response.body.aggregations.monthly_usage.buckets;
    const monthlyWordCounts: { [month: string]: { [word: string]: number } } = {};

    // Process aggregation results
    for (const bucket of buckets) {
      const month = bucket.key_as_string;
      monthlyWordCounts[month] = {};
      for (const wordBucket of bucket.word_counts.buckets) {
        monthlyWordCounts[month][wordBucket.key] = wordBucket.doc_count;
      }
    }

    console.log('Monthly word counts:', JSON.stringify(monthlyWordCounts, null, 2));

    // Detect spikes
    const sortedMonths = Object.keys(monthlyWordCounts).sort();
    for (let i = 1; i < sortedMonths.length; i++) {
      const previousMonth = sortedMonths[i - 1];
      const currentMonth = sortedMonths[i];

      console.log(`\nComparing ${previousMonth} to ${currentMonth}:`);

      for (const word in monthlyWordCounts[currentMonth]) {
        const currentCount = monthlyWordCounts[currentMonth][word];
        const previousCount = monthlyWordCounts[previousMonth][word] || 0;

        if (previousCount > 0 && currentCount >= previousCount * 10) {
          console.log(
            `  - Spike detected for "${word}": ${previousCount} -> ${currentCount} (x${(currentCount / previousCount).toFixed(2)})`
          );
        }
      }
    }
  } catch (error) {
    console.error('An error occurred during the search:', error);
  }
}

async function main() {
  try {
    await searchSpikes();
  } catch (error) {
    console.error('An unexpected error occurred:', error);
    process.exit(1);
  }
}

main();