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
                 nlist: int = 4096, nprobe: int = 64, save_every: int = 100, use_pq: bool = True, pq_m: int = 64):
        self.db_path = db_path
        self.faiss_index_path = faiss_index_path
        self.batch_size = batch_size
        self.nlist = nlist  # Number of clusters for IVF
        self.nprobe = nprobe  # Number of clusters to search
        self.save_every = save_every  # Save index every N batches
        self.use_pq = use_pq  # Use Product Quantization for compression
        self.pq_m = pq_m  # Number of PQ segments (must divide dimension)
        self.db_conn = None
        self.faiss_index = None
        self.quantizer = None  # For IVF training
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
                topic TEXT,
                thread_length INTEGER
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
        """Initialize FAISS index with IVF for efficient approximate search with range support."""
        self.dimension = dimension
        
        # Create quantizer for IVF (uses flat index for cluster centroids)
        self.quantizer = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
        
        # Create IVF index with the quantizer
        # nlist: number of clusters (higher = better accuracy, more memory)
        # nprobe will be set later during search
        self.faiss_index = faiss.IndexIVFFlat(self.quantizer, dimension, self.nlist, faiss.METRIC_INNER_PRODUCT)
        
        logger.info(f"FAISS IVF index initialized with dimension: {dimension}, nlist: {self.nlist}")
        logger.info("Note: Index needs training before adding vectors")
    
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
                    post.get('topic'),
                    int(post.get('thread_length', 1)) if post.get('thread_length') else 1
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
                    username, temporal_subset, topic, thread_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        username, temporal_subset, topic, thread_length
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    post.get('topic'),
                    int(post.get('thread_length', 1)) if post.get('thread_length') else 1
                ))
                successful += 1
            except Exception as e:
                logger.error(f"Error inserting post {post.get('tweet_id', 'unknown')}: {e}")
                continue
        
        self.db_conn.commit()
        logger.info(f"Inserted {successful} posts individually as fallback")
    
    def ingest_file(self, jsonl_file: str):
        """Main ingestion method with streaming batch processing and IVF training."""
        logger.info(f"Starting streaming ingestion of {jsonl_file} with batch size {self.batch_size}")
        
        # Setup SQLite
        self.setup_sqlite()
        
        faiss_initialized = False
        faiss_trained = False
        training_vectors = []
        total_batches = 0
        
        # Single pass: collect training data early, then stream the rest
        logger.info("Processing file with streaming IVF training...")
        
        for batch_posts, current_line in self.load_jsonl_batches(jsonl_file):
            if not batch_posts:
                continue
                
            total_batches += 1
            if total_batches % 50 == 0:  # Less frequent logging
                logger.info(f"Processing batch {total_batches} (lines up to {current_line}, {len(batch_posts)} posts)")
            
            # Initialize FAISS on first batch with vectors
            if not faiss_initialized and batch_posts[0].get('full_text_vector'):
                first_vector = np.array(batch_posts[0]['full_text_vector'])
                self.setup_faiss(len(first_vector))
                faiss_initialized = True
            
            # Extract vectors from current batch
            vectors = self.extract_vectors(batch_posts) if faiss_initialized else None
            
            # Collect training data from early batches
            if faiss_initialized and not faiss_trained and vectors is not None and vectors.size > 0:
                training_vectors.append(vectors)
                total_training_vectors = sum(len(v) for v in training_vectors)
                
                # Train when we have enough vectors (aim for 100-200 * nlist)
                if total_training_vectors >= max(100 * self.nlist, 500000):
                    self._train_index(training_vectors)
                    faiss_trained = True
                    training_vectors = []  # Free training data memory immediately
                    logger.info("IVF index training completed, continuing with streaming vector addition")
            
            # Add vectors to index if trained (streaming approach)
            if faiss_trained and vectors is not None and vectors.size > 0:
                self.faiss_index.add(vectors)
                if total_batches % 100 == 0:  # Log every 100 batches
                    logger.info(f"Added {len(vectors)} vectors to FAISS index (batch {total_batches})")
            
            # Insert posts into SQLite
            self.insert_posts_batch(batch_posts)
            self.total_processed += len(batch_posts)
            
            # Periodic saving to prevent data loss
            if faiss_trained and total_batches % self.save_every == 0:
                self._save_index_checkpoint(total_batches)
            
            # Log progress every 200 batches to reduce log spam
            if total_batches % 200 == 0:
                logger.info(f"Progress: {self.total_processed} posts processed in {total_batches} batches")
        
        # Handle case where dataset is too small and training didn't happen
        if faiss_initialized and not faiss_trained:
            if training_vectors:
                self._train_index(training_vectors)
                faiss_trained = True
                training_vectors = []
                
                # Second pass only needed for small datasets
                logger.info("Training completed on small dataset, making second pass...")
                self._add_all_vectors_second_pass(jsonl_file)
            else:
                logger.error("No vectors found for training")
                return
        
        if total_batches == 0:
            logger.error("No valid batches processed from file")
            return
        
        # Save FAISS index if it was created
        if faiss_trained and self.faiss_index:
            # Set nprobe for search performance
            self.faiss_index.nprobe = self.nprobe
            faiss.write_index(self.faiss_index, self.faiss_index_path)
            logger.info(f"FAISS IVF index saved to {self.faiss_index_path} with nprobe={self.nprobe}")
            logger.info(f"Index contains {self.faiss_index.ntotal} vectors")
        else:
            logger.warning("No vectors found in posts, skipping FAISS indexing")
        
        logger.info(f"Ingestion completed successfully. Total processed: {self.total_processed} posts in {total_batches} batches")
    
    def _add_all_vectors_second_pass(self, jsonl_file: str):
        """Second pass to add all vectors when training happened on small dataset."""
        logger.info("Second pass: adding all vectors to trained index...")
        batch_count = 0
        total_added = 0
        
        for batch_posts, current_line in self.load_jsonl_batches(jsonl_file):
            if not batch_posts:
                continue
                
            batch_count += 1
            vectors = self.extract_vectors(batch_posts)
            if vectors.size > 0:
                self.faiss_index.add(vectors)
                total_added += len(vectors)
                if batch_count % 50 == 0:
                    logger.info(f"Added vectors from {batch_count} batches ({total_added} total vectors)")
        
        logger.info(f"Second pass completed: added {total_added} vectors from {batch_count} batches")
    
    def _train_index(self, training_vectors: List[np.ndarray]):
        """Train the IVF index on collected vectors."""
        # Concatenate all training vectors
        all_training = np.vstack(training_vectors)
        logger.info(f"Training IVF index on {len(all_training)} vectors...")
        
        # Train the index
        self.faiss_index.train(all_training)
        logger.info("IVF index training completed")
    
    def _save_index_checkpoint(self, batch_num: int):
        """Save index checkpoint to prevent data loss."""
        if self.faiss_index and self.faiss_index.is_trained:
            # Set nprobe for search performance
            self.faiss_index.nprobe = self.nprobe
            faiss.write_index(self.faiss_index, self.faiss_index_path)
            logger.info(f"Checkpoint saved at batch {batch_num}: {self.faiss_index.ntotal} vectors indexed")
    
    def close(self):
        """Close database connections."""
        if self.db_conn:
            self.db_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest JSONL file into FAISS IVF and SQLite FTS")
    parser.add_argument("jsonl_file", help="Path to JSONL file")
    parser.add_argument("--db", default="posts.db", help="SQLite database path")
    parser.add_argument("--faiss-index", default="posts_faiss.index", help="FAISS index path")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for processing (default: 10000)")
    parser.add_argument("--nlist", type=int, default=4096, help="IVF nlist parameter - number of clusters (default: 4096)")
    parser.add_argument("--nprobe", type=int, default=64, help="IVF nprobe parameter - clusters to search (default: 64)")
    parser.add_argument("--save-every", type=int, default=100, help="Save index checkpoint every N batches (default: 100)")
    
    args = parser.parse_args()
    
    if not Path(args.jsonl_file).exists():
        logger.error(f"File not found: {args.jsonl_file}")
        return
    
    ingester = PostIngester(args.db, args.faiss_index, args.batch_size, args.nlist, args.nprobe, args.save_every)
    try:
        ingester.ingest_file(args.jsonl_file)
    finally:
        ingester.close()


if __name__ == "__main__":
    main()