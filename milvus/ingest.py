#!/usr/bin/env python3

import json
import argparse
from datetime import datetime
from pymilvus import MilvusClient, model
from typing import List, Dict, Any
import sys
import os

class TwitterDataIngester:
    def __init__(self, db_path: str = "data.db", collection_name: str = "twitter_posts"):
        self.client = MilvusClient(db_path)
        self.collection_name = collection_name
        self.embedding_fn = model.DefaultEmbeddingFunction()
        self.batch_size = 1000
        
    def create_collection(self):
        """Create Milvus collection with appropriate schema for twitter data."""
        if self.client.has_collection(collection_name=self.collection_name):
            print(f"Dropping existing collection: {self.collection_name}")
            self.client.drop_collection(collection_name=self.collection_name)
        
        print(f"Creating collection: {self.collection_name}")
        self.client.create_collection(
            collection_name=self.collection_name,
            dimension=768,  # DefaultEmbeddingFunction produces 768-dim vectors
        )
        
    def extract_date_components(self, updated_at: str) -> Dict[str, int]:
        """Extract year, month, day from updated_at timestamp."""
        try:
            # Parse the timestamp format: "2025-01-25 18:49:16.732494+00:00"
            dt = datetime.fromisoformat(updated_at.replace('+00:00', ''))
            return {
                'year': dt.year,
                'month': dt.month,
                'day': dt.day
            }
        except ValueError as e:
            print(f"Error parsing date '{updated_at}': {e}")
            return {'year': 0, 'month': 0, 'day': 0}
    
    def process_batch(self, records: List[Dict[str, Any]], start_id: int) -> List[Dict[str, Any]]:
        """Process a batch of records for insertion into Milvus."""
        batch_data = []
        texts_to_embed = []
        
        # Extract texts for batch embedding generation
        for record in records:
            full_text = record.get('full_text', '')
            if full_text:
                texts_to_embed.append(full_text)
            else:
                texts_to_embed.append("")  # Handle missing text
        
        # Generate embeddings for all texts in batch
        if texts_to_embed:
            vectors = self.embedding_fn.encode_documents(texts_to_embed)
        else:
            vectors = []
        
        # Create batch data for insertion
        for i, record in enumerate(records):
            date_components = self.extract_date_components(record.get('updated_at', ''))
            
            data_item = {
                'id': start_id + i,  # Use integer ID for Milvus
                'tweet_id': record.get('tweet_id', ''),
                'full_text': record.get('full_text', ''),
                'year': date_components['year'],
                'month': date_components['month'],
                'day': date_components['day'],
                'vector': vectors[i] if i < len(vectors) else [0.0] * 768
            }
            batch_data.append(data_item)
        
        return batch_data
    
    def ingest_jsonl_file(self, file_path: str):
        """Ingest data from JSONL file into Milvus collection."""
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            return
        
        print(f"Starting ingestion from: {file_path}")
        
        batch = []
        total_processed = 0
        total_errors = 0
        current_id = 1  # Start IDs from 1
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        batch.append(record)
                        
                        # Process batch when it reaches batch_size
                        if len(batch) >= self.batch_size:
                            processed_batch = self.process_batch(batch, current_id)
                            
                            # Insert batch into Milvus
                            try:
                                res = self.client.insert(
                                    collection_name=self.collection_name, 
                                    data=processed_batch
                                )
                                total_processed += len(batch)
                                current_id += len(batch)  # Update ID counter
                                print(f"Processed {total_processed} records...")
                            except Exception as e:
                                print(f"Error inserting batch at line {line_num}: {e}")
                                total_errors += len(batch)
                            
                            batch = []  # Reset batch
                            
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error at line {line_num}: {e}")
                        total_errors += 1
                        continue
                
                # Process remaining records in final batch
                if batch:
                    processed_batch = self.process_batch(batch, current_id)
                    try:
                        res = self.client.insert(
                            collection_name=self.collection_name, 
                            data=processed_batch
                        )
                        total_processed += len(batch)
                    except Exception as e:
                        print(f"Error inserting final batch: {e}")
                        total_errors += len(batch)
        
        except Exception as e:
            print(f"Error reading file: {e}")
            return
        
        print(f"\nIngestion complete!")
        print(f"Total records processed: {total_processed}")
        print(f"Total errors: {total_errors}")
        
        # Display collection stats
        collection_info = self.client.describe_collection(collection_name=self.collection_name)
        print(f"Collection '{self.collection_name}' now contains data with schema: {collection_info}")

def main():
    parser = argparse.ArgumentParser(description="Ingest Twitter data from JSONL into Milvus")
    parser.add_argument("input_file", help="Path to the input JSONL file")
    parser.add_argument("--db", default="data.db", help="Path to Milvus database file (default: data.db)")
    parser.add_argument("--collection", default="twitter_posts", help="Collection name (default: twitter_posts)")
    
    args = parser.parse_args()
    
    # Initialize ingester
    ingester = TwitterDataIngester(db_path=args.db, collection_name=args.collection)
    
    # Create collection
    ingester.create_collection()
    
    # Ingest data
    ingester.ingest_jsonl_file(args.input_file)

if __name__ == "__main__":
    main()