#!/usr/bin/env python3
"""
Ingest JSONL file into FAISS index and SQLite FTS database.
Supports semantic search and full-text search for tweet data.
"""

import json
import sqlite3
import numpy as np
import faiss
from typing import List, Dict, Any
import argparse
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostIngester:
    def __init__(self, db_path: str = "posts.db", faiss_index_path: str = "posts_faiss.index", batch_size: int = 10000, 
                 hnsw_m: int = 16, hnsw_ef_construction: int = 200):
        self.db_path = db_path
        self.faiss_index_path = faiss_index_path
        self.batch_size = batch_size
        self.hnsw_m = hnsw_m
        self.hnsw_ef_construction = hnsw_ef_construction
        self.db_conn = None
        self.faiss_index = None
        self.dimension = 512  # Will be updated based on actual vector dimension
        self.total_processed = 0
        
    def setup_sqlite(self):
        """Initialize SQLite database with FTS support."""
        self.db_conn = sqlite3.connect(self.db_path)
        cursor = self.db_conn.cursor()
        
        # Create main posts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE NOT NULL,
                account_id TEXT,
                created_at TEXT,
                full_text TEXT,
                retweet_count INTEGER,
                favorite_count INTEGER,
                reply_to_tweet_id TEXT,
                reply_to_user_id TEXT,
                reply_to_username TEXT,
                archive_upload_id TEXT,
                updated_at TEXT,
                username TEXT,
                temporal_subset TEXT,
                topic TEXT
            )
        ''')
        
        # Create FTS virtual table for full-text search
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(
                tweet_id,
                full_text,
                username,
                topic,
                content='posts',
                content_rowid='id'
            )
        ''')
        
        # Create triggers to keep FTS table in sync
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS posts_ai AFTER INSERT ON posts BEGIN
                INSERT INTO posts_fts(rowid, tweet_id, full_text, username, topic) 
                VALUES (new.id, new.tweet_id, new.full_text, new.username, new.topic);
            END
        ''')
        
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS posts_ad AFTER DELETE ON posts BEGIN
                INSERT INTO posts_fts(posts_fts, rowid, tweet_id, full_text, username, topic) 
                VALUES('delete', old.id, old.tweet_id, old.full_text, old.username, old.topic);
            END
        ''')
        
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS posts_au AFTER UPDATE ON posts BEGIN
                INSERT INTO posts_fts(posts_fts, rowid, tweet_id, full_text, username, topic) 
                VALUES('delete', old.id, old.tweet_id, old.full_text, old.username, old.topic);
                INSERT INTO posts_fts(rowid, tweet_id, full_text, username, topic) 
                VALUES (new.id, new.tweet_id, new.full_text, new.username, new.topic);
            END
        ''')
        
        self.db_conn.commit()
        logger.info(f"SQLite database initialized: {self.db_path}")
    
    def setup_faiss(self, dimension: int):
        """Initialize FAISS index with HNSW for efficient approximate search."""
        self.dimension = dimension
        # Using IndexHNSWFlat for fast approximate search with inner product (cosine similarity)
        # M: number of bidirectional links for each node (higher = better accuracy, more memory)
        # efConstruction: size of dynamic candidate list (higher = better accuracy, slower build)
        self.faiss_index = faiss.IndexHNSWFlat(dimension, self.hnsw_m)
        self.faiss_index.metric_type = faiss.METRIC_INNER_PRODUCT
        self.faiss_index.hnsw.efConstruction = self.hnsw_ef_construction
        logger.info(f"FAISS HNSW index initialized with dimension: {dimension}, M: {self.hnsw_m}, efConstruction: {self.hnsw_ef_construction}")
    
    def load_jsonl_batches(self, file_path: str):
        """Generator that yields batches of posts from JSONL file."""
        batch = []
        line_num = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line_num += 1
                try:
                    post = json.loads(line.strip())
                    batch.append(post)
                    
                    if len(batch) >= self.batch_size:
                        yield batch, line_num
                        batch = []
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                    continue
            
            # Yield remaining posts in the last batch
            if batch:
                yield batch, line_num
    
    def extract_vectors(self, posts: List[Dict[str, Any]]) -> np.ndarray:
        """Extract and normalize vectors from posts."""
        vectors = []
        for post in posts:
            if 'full_text_vector' in post and post['full_text_vector']:
                vector = np.array(post['full_text_vector'], dtype=np.float32)
                # Normalize for cosine similarity
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = vector / norm
                vectors.append(vector)
            else:
                logger.warning(f"No vector found for tweet_id: {post.get('tweet_id', 'unknown')}")
                # Create zero vector as placeholder
                vectors.append(np.zeros(self.dimension, dtype=np.float32))
        
        return np.array(vectors)
    
    def insert_posts_batch(self, posts: List[Dict[str, Any]]):
        """Insert a batch of posts into SQLite database."""
        cursor = self.db_conn.cursor()
        
        # Use executemany for better performance
        posts_data = []
        for post in posts:
            try:
                posts_data.append((
                    post.get('tweet_id'),
                    post.get('account_id'),
                    post.get('created_at'),
                    post.get('full_text'),
                    int(post.get('retweet_count', 0)) if post.get('retweet_count') else 0,
                    int(post.get('favorite_count', 0)) if post.get('favorite_count') else 0,
                    post.get('reply_to_tweet_id'),
                    post.get('reply_to_user_id'),
                    post.get('reply_to_username'),
                    post.get('archive_upload_id'),
                    post.get('updated_at'),
                    post.get('username'),
                    post.get('temporal_subset'),
                    post.get('topic')
                ))
            except Exception as e:
                logger.error(f"Error preparing post {post.get('tweet_id', 'unknown')}: {e}")
                continue
        
        try:
            cursor.executemany('''
                INSERT OR REPLACE INTO posts (
                    tweet_id, account_id, created_at, full_text, retweet_count,
                    favorite_count, reply_to_tweet_id, reply_to_user_id, 
                    reply_to_username, archive_upload_id, updated_at, 
                    username, temporal_subset, topic
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', posts_data)
            self.db_conn.commit()
            logger.info(f"Inserted batch of {len(posts_data)} posts into SQLite")
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            # Rollback and try individual inserts as fallback
            self.db_conn.rollback()
            self._insert_posts_individually(posts)
    
    def _insert_posts_individually(self, posts: List[Dict[str, Any]]):
        """Fallback method to insert posts individually."""
        cursor = self.db_conn.cursor()
        successful = 0
        
        for post in posts:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO posts (
                        tweet_id, account_id, created_at, full_text, retweet_count,
                        favorite_count, reply_to_tweet_id, reply_to_user_id, 
                        reply_to_username, archive_upload_id, updated_at, 
                        username, temporal_subset, topic
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post.get('tweet_id'),
                    post.get('account_id'),
                    post.get('created_at'),
                    post.get('full_text'),
                    int(post.get('retweet_count', 0)) if post.get('retweet_count') else 0,
                    int(post.get('favorite_count', 0)) if post.get('favorite_count') else 0,
                    post.get('reply_to_tweet_id'),
                    post.get('reply_to_user_id'),
                    post.get('reply_to_username'),
                    post.get('archive_upload_id'),
                    post.get('updated_at'),
                    post.get('username'),
                    post.get('temporal_subset'),
                    post.get('topic')
                ))
                successful += 1
            except Exception as e:
                logger.error(f"Error inserting post {post.get('tweet_id', 'unknown')}: {e}")
                continue
        
        self.db_conn.commit()
        logger.info(f"Inserted {successful} posts individually as fallback")
    
    def ingest_file(self, jsonl_file: str):
        """Main ingestion method with batch processing."""
        logger.info(f"Starting batch ingestion of {jsonl_file} with batch size {self.batch_size}")
        
        # Setup SQLite
        self.setup_sqlite()
        
        faiss_initialized = False
        batch_count = 0
        
        # Process file in batches
        for batch_posts, current_line in self.load_jsonl_batches(jsonl_file):
            if not batch_posts:
                continue
                
            batch_count += 1
            logger.info(f"Processing batch {batch_count} (lines up to {current_line}, {len(batch_posts)} posts)")
            
            # Initialize FAISS on first batch with vectors
            if not faiss_initialized and batch_posts[0].get('full_text_vector'):
                first_vector = np.array(batch_posts[0]['full_text_vector'])
                self.setup_faiss(len(first_vector))
                faiss_initialized = True
            
            # Process vectors if FAISS is initialized
            if faiss_initialized:
                vectors = self.extract_vectors(batch_posts)
                if vectors.size > 0:
                    self.faiss_index.add(vectors)
                    logger.info(f"Added {len(vectors)} vectors to FAISS index (batch {batch_count})")
            
            # Insert posts into SQLite
            self.insert_posts_batch(batch_posts)
            
            self.total_processed += len(batch_posts)
            logger.info(f"Total processed so far: {self.total_processed} posts")
        
        if batch_count == 0:
            logger.error("No valid batches processed from file")
            return
        
        # Save FAISS index if it was created
        if faiss_initialized and self.faiss_index:
            faiss.write_index(self.faiss_index, self.faiss_index_path)
            logger.info(f"FAISS index saved to {self.faiss_index_path}")
        else:
            logger.warning("No vectors found in posts, skipping FAISS indexing")
        
        logger.info(f"Ingestion completed successfully. Total processed: {self.total_processed} posts in {batch_count} batches")
    
    def close(self):
        """Close database connections."""
        if self.db_conn:
            self.db_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest JSONL file into FAISS and SQLite FTS")
    parser.add_argument("jsonl_file", help="Path to JSONL file")
    parser.add_argument("--db", default="posts.db", help="SQLite database path")
    parser.add_argument("--faiss-index", default="posts_faiss.index", help="FAISS index path")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for processing (default: 10000)")
    parser.add_argument("--hnsw-m", type=int, default=16, help="HNSW M parameter - bidirectional links per node (default: 16)")
    parser.add_argument("--hnsw-ef-construction", type=int, default=200, help="HNSW efConstruction parameter - build time accuracy (default: 200)")
    
    args = parser.parse_args()
    
    if not Path(args.jsonl_file).exists():
        logger.error(f"File not found: {args.jsonl_file}")
        return
    
    ingester = PostIngester(args.db, args.faiss_index, args.batch_size, args.hnsw_m, args.hnsw_ef_construction)
    try:
        ingester.ingest_file(args.jsonl_file)
    finally:
        ingester.close()


if __name__ == "__main__":
    main()