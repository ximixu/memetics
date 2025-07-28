#!/usr/bin/env python3

import csv
import requests
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

class OpenSearchCSVIngester:
    def __init__(self, opensearch_url: str = "http://localhost:9200", index_name: str = "tweets"):
        self.opensearch_url = opensearch_url
        self.headers = {'Content-Type': 'application/json'}
        self.index_name = index_name
        self.pipeline_name = "tweet_ingest"
        self.batch_size = 100
        
        # CSV column definitions
        self.csv_columns = [
            'tweet_id', 'account_id', 'created_at', 'full_text', 'retweet_count',
            'favorite_count', 'reply_to_tweet_id', 'reply_to_user_id', 
            'reply_to_username', 'archive_upload_id', 'updated_at', 'username',
            'temporal_subset', 'topic'
        ]
    
    def check_opensearch_connection(self) -> bool:
        """Check if OpenSearch is accessible"""
        try:
            response = requests.get(f"{self.opensearch_url}/")
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to OpenSearch: {e}")
            return False
    
    def check_pipeline_exists(self) -> bool:
        """Check if the tweet_ingest pipeline exists"""
        try:
            response = requests.get(f"{self.opensearch_url}/_ingest/pipeline/{self.pipeline_name}")
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Error checking pipeline: {e}")
            return False
    
    def check_index_exists(self) -> bool:
        """Check if the target index exists"""
        try:
            response = requests.head(f"{self.opensearch_url}/{self.index_name}")
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Error checking index: {e}")
            return False
    
    def create_index(self) -> bool:
        """Create the index with proper mappings for all CSV columns and embeddings"""
        print(f"Creating index '{self.index_name}' with mappings...")
        
        index_mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "shingle_analyzer": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "stop", "shingle_filter"]
                        }
                    },
                    "filter": {
                        "shingle_filter": {
                            "type": "shingle",
                            "min_shingle_size": 2,
                            "max_shingle_size": 4,
                            "output_unigrams": True
                        }
                    }
                },
                "index": {
                    "knn": True,
                    "knn.algo_param.ef_search": 100
                }
            },
            "mappings": {
                "properties": {
                    "tweet_id": {"type": "keyword"},
                    "account_id": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "full_text": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "shingle": {
                                "type": "text",
                                "analyzer": "shingle_analyzer"
                            }
                        }
                    },
                    "full_text_embedding": {
                        "type": "knn_vector",
                        "dimension": 384,
                        "space_type": "cosinesimil",
                        "method": {
                            "name": "hnsw",
                            "engine": "faiss",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 16
                            }
                        }
                    },
                    "retweet_count": {"type": "integer"},
                    "favorite_count": {"type": "integer"},
                    "reply_to_tweet_id": {"type": "keyword"},
                    "reply_to_user_id": {"type": "keyword"},
                    "reply_to_username": {"type": "keyword"},
                    "archive_upload_id": {"type": "keyword"},
                    "updated_at": {"type": "date"},
                    "username": {"type": "keyword"},
                    "temporal_subset": {"type": "keyword"},
                    "topic": {"type": "keyword"}
                }
            }
        }
        
        try:
            response = requests.put(
                f"{self.opensearch_url}/{self.index_name}",
                headers=self.headers,
                json=index_mapping
            )
            
            if response.status_code in [200, 201]:
                print(f"Index '{self.index_name}' created successfully")
                return True
            else:
                print(f"Failed to create index: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error creating index: {e}")
            return False
    
    def parse_csv_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Parse a CSV row and convert data types appropriately"""
        parsed_row = {}
        
        for column in self.csv_columns:
            value = row.get(column, "").strip()
            
            if column in ['tweet_id', 'account_id', 'reply_to_tweet_id', 'reply_to_user_id', 
                         'reply_to_username', 'archive_upload_id', 'username', 'temporal_subset', 'topic']:
                # Keyword fields - keep as strings, handle empty values
                parsed_row[column] = value if value else None
                
            elif column in ['retweet_count', 'favorite_count']:
                # Integer fields
                try:
                    parsed_row[column] = int(value) if value else 0
                except ValueError:
                    print(f"Warning: Invalid integer value '{value}' for {column}, using 0")
                    parsed_row[column] = 0
                    
            elif column in ['created_at', 'updated_at']:
                # Date fields - normalize to ISO8601 format
                if value:
                    try:
                        # Handle format: '2023-07-28 21:14:42+00:00' -> '2023-07-28T21:14:42+00:00'
                        if '+' in value and ' ' in value and value.count(':') >= 2:
                            # Replace space with 'T' to make it proper ISO8601
                            parsed_row[column] = value.replace(' ', 'T')
                        elif 'T' in value or 'Z' in value:
                            # Already in ISO format
                            parsed_row[column] = value
                        else:
                            # Assume YYYY-MM-DD HH:MM:SS format (add Z for UTC)
                            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            parsed_row[column] = dt.isoformat() + 'Z'
                    except ValueError as e:
                        print(f"Warning: Invalid date format '{value}' for {column}: {e}")
                        parsed_row[column] = None
                else:
                    parsed_row[column] = None
                    
            elif column == 'full_text':
                # Text field - keep as is but ensure it's not empty for embeddings
                parsed_row[column] = value if value else ""
                
            else:
                # Default case
                parsed_row[column] = value if value else None
        
        return parsed_row
    
    def read_csv_file(self, csv_file_path: str) -> List[Dict[str, Any]]:
        """Read and parse CSV file into a list of documents"""
        documents = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                # Use DictReader to automatically map columns
                reader = csv.DictReader(file)
                
                # Validate that all required columns are present
                missing_columns = set(self.csv_columns) - set(reader.fieldnames or [])
                if missing_columns:
                    print(f"Error: Missing required columns: {missing_columns}")
                    return []
                
                print(f"Found columns: {reader.fieldnames}")
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 since row 1 is header
                    try:
                        parsed_row = self.parse_csv_row(row)
                        documents.append(parsed_row)
                        
                        if row_num % 1000 == 0:
                            print(f"Parsed {row_num - 1} rows...")
                            
                    except Exception as e:
                        print(f"Error parsing row {row_num}: {e}")
                        continue
                        
        except FileNotFoundError:
            print(f"Error: CSV file '{csv_file_path}' not found")
            return []
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return []
        
        print(f"Successfully parsed {len(documents)} documents from CSV")
        return documents
    
    def bulk_ingest_documents(self, documents: List[Dict[str, Any]]) -> bool:
        """Ingest documents using OpenSearch Bulk API with the tweet_ingest pipeline"""
        if not documents:
            print("No documents to ingest")
            return True
        
        print(f"Starting bulk ingestion of {len(documents)} documents...")
        total_ingested = 0
        
        # Process documents in batches
        for i in range(0, len(documents), self.batch_size):
            batch = documents[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(documents) + self.batch_size - 1) // self.batch_size
            
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} documents)...")
            
            # Create bulk request body
            bulk_body = []
            for doc in batch:
                # Add index action
                bulk_body.append(json.dumps({"index": {"_index": self.index_name}}))
                # Add document
                bulk_body.append(json.dumps(doc))
            
            bulk_data = "\n".join(bulk_body) + "\n"
            
            # Send bulk request with pipeline parameter
            success = self.send_bulk_request(bulk_data, batch_num, retries=3)
            if success:
                total_ingested += len(batch)
                print(f"Successfully ingested batch {batch_num} ({total_ingested}/{len(documents)} total)")
            else:
                print(f"Failed to ingest batch {batch_num}")
                return False
        
        print(f"Bulk ingestion completed. Total documents ingested: {total_ingested}")
        return True
    
    def send_bulk_request(self, bulk_data: str, batch_num: int, retries: int = 3) -> bool:
        """Send a bulk request to OpenSearch with retry logic"""
        for attempt in range(retries):
            try:
                response = requests.post(
                    f"{self.opensearch_url}/_bulk?pipeline={self.pipeline_name}",
                    headers={'Content-Type': 'application/x-ndjson'},
                    data=bulk_data,
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result.get('errors', False):
                        print(f"Batch {batch_num} had errors:")
                        error_count = 0
                        for item in result.get('items', []):
                            if 'index' in item and 'error' in item['index']:
                                error_count += 1
                                error_info = item['index']['error']
                                print(f"  Error: {error_info.get('type', 'unknown')} - {error_info.get('reason', 'no reason')}")
                        
                        if error_count > len(result.get('items', [])) / 2:
                            print(f"Too many errors in batch {batch_num} ({error_count} errors)")
                            return False
                        else:
                            print(f"Batch {batch_num} completed with {error_count} errors (continuing)")
                            return True
                    else:
                        return True
                else:
                    print(f"Bulk request failed with status {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Request error on attempt {attempt + 1}/{retries}: {e}")
                
            if attempt < retries - 1:
                print(f"Retrying batch {batch_num} in 5 seconds...")
                import time
                time.sleep(5)
        
        print(f"Failed to send batch {batch_num} after {retries} attempts")
        return False
    
    def ingest_csv(self, csv_file_path: str) -> bool:
        """Main method to ingest a CSV file into OpenSearch with embeddings"""
        print(f"Starting CSV ingestion for file: {csv_file_path}")
        print(f"Target index: {self.index_name}")
        print(f"Pipeline: {self.pipeline_name}")
        print(f"OpenSearch URL: {self.opensearch_url}")
        
        # Step 1: Check OpenSearch connection
        if not self.check_opensearch_connection():
            print("Failed to connect to OpenSearch")
            return False
        print("✓ Connected to OpenSearch successfully")
        
        # Step 2: Check if pipeline exists
        if not self.check_pipeline_exists():
            print(f"Error: Pipeline '{self.pipeline_name}' does not exist")
            print("Please run setup_opensearch_model.py first to create the pipeline")
            return False
        print(f"✓ Pipeline '{self.pipeline_name}' exists")
        
        # Step 3: Check and create index if needed
        if not self.check_index_exists():
            print(f"Index '{self.index_name}' does not exist, creating...")
            if not self.create_index():
                print("Failed to create index")
                return False
        print(f"✓ Index '{self.index_name}' is ready")
        
        # Step 4: Read and parse CSV file
        documents = self.read_csv_file(csv_file_path)
        if not documents:
            print("No documents to ingest")
            return False
        print(f"✓ Successfully parsed {len(documents)} documents")
        
        # Step 5: Bulk ingest documents with embeddings
        if not self.bulk_ingest_documents(documents):
            print("Failed to ingest documents")
            return False
        print("✓ Successfully ingested all documents")
        
        # Step 6: Refresh index and show final count
        try:
            refresh_response = requests.post(f"{self.opensearch_url}/{self.index_name}/_refresh")
            if refresh_response.status_code == 200:
                print("✓ Index refreshed successfully")
            
            count_response = requests.get(f"{self.opensearch_url}/{self.index_name}/_count")
            if count_response.status_code == 200:
                count = count_response.json().get('count', 0)
                print(f"✓ Final document count in index '{self.index_name}': {count}")
            
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not refresh index or get count: {e}")
        
        print("CSV ingestion completed successfully!")
        return True


def main():
    if len(sys.argv) != 2:
        print("Usage: python ingest_csv.py <csv_file_path>")
        print("\nExample: python ingest_csv.py tweets.csv")
        print("\nThe CSV file should contain these columns:")
        print("tweet_id,account_id,created_at,full_text,retweet_count,favorite_count,")
        print("reply_to_tweet_id,reply_to_user_id,reply_to_username,archive_upload_id,")
        print("updated_at,username,temporal_subset,topic")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' not found")
        sys.exit(1)
    
    # Initialize ingester
    opensearch_url = os.getenv('OPENSEARCH_URL', 'http://localhost:9200')
    index_name = os.getenv('INDEX_NAME', 'tweets')
    
    ingester = OpenSearchCSVIngester(opensearch_url=opensearch_url, index_name=index_name)
    
    # Run ingestion
    try:
        success = ingester.ingest_csv(csv_file_path)
        if success:
            print("\n🎉 CSV ingestion completed successfully!")
            print(f"Documents are now indexed in '{index_name}' with embeddings generated for the 'full_text' field")
        else:
            print("\n❌ CSV ingestion failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()