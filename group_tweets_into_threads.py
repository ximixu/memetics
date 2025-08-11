#!/usr/bin/env python3
"""
Scalable script to group tweets from JSONL into threads and filter out retweets.
Adapted from the logic in embeddings-test/get_tweets_and_threads.ipynb
Optimized for large datasets (multi-GB) with streaming processing.
"""

import json
import sys
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
import time

class UnionFind:
    """Union-Find data structure for efficient thread root detection."""
    def __init__(self):
        self.parent = {}
        self.rank = {}
    
    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x
        
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]
    
    def union(self, x, y):
        root_x, root_y = self.find(x), self.find(y)
        if root_x != root_y:
            # Union by rank
            if self.rank[root_x] < self.rank[root_y]:
                self.parent[root_x] = root_y
            elif self.rank[root_x] > self.rank[root_y]:
                self.parent[root_y] = root_x
            else:
                self.parent[root_y] = root_x
                self.rank[root_x] += 1

def stream_build_reply_relationships(file_path: str, chunk_size: int = 10000) -> Tuple[Dict[str, str], Set[str], int]:
    """Stream process JSONL to build reply relationships efficiently.
    
    Returns:
        (reply_mapping, all_tweet_ids, total_tweets)
    """
    reply_to_parent = {}  # tweet_id -> parent_tweet_id
    all_tweet_ids = set()
    total_tweets = 0
    retweets_filtered = 0
    
    print("Phase 1: Building reply relationships...")
    start_time = time.time()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                tweet = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Skipping malformed JSON at line {line_num}")
                continue
            
            tweet_id = tweet.get('tweet_id')
            tweet_text = tweet.get('full_text', '')
            
            if not tweet_id:
                continue
                
            total_tweets += 1
            
            # Filter retweets early to save memory
            if is_retweet(tweet_text):
                retweets_filtered += 1
                continue
                
            all_tweet_ids.add(tweet_id)
            
            # Build reply relationship (no account filtering - cross-account threads)
            reply_to_tweet_id = tweet.get('reply_to_tweet_id')
            if reply_to_tweet_id:
                reply_to_parent[tweet_id] = reply_to_tweet_id
            
            # Progress reporting
            if line_num % chunk_size == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed
                print(f"  Processed {line_num:,} lines ({rate:.0f} lines/sec), "
                      f"found {len(reply_to_parent):,} reply relationships")
    
    elapsed = time.time() - start_time
    print(f"Phase 1 complete: {total_tweets:,} total tweets, "
          f"{retweets_filtered:,} retweets filtered ({retweets_filtered/total_tweets*100:.1f}%), "
          f"{len(all_tweet_ids):,} tweets remaining")
    print(f"Reply relationships: {len(reply_to_parent):,}, Time: {elapsed:.1f}s")
    
    return reply_to_parent, all_tweet_ids, total_tweets

def is_retweet(tweet_text: str) -> bool:
    """Check if a tweet is a retweet based on text content."""
    if not tweet_text:
        return False
    tweet_text = tweet_text.strip()
    return (tweet_text.startswith('RT <USER_') or 
            tweet_text.startswith('RT @ ') or 
            tweet_text.startswith('RT @'))

def find_thread_roots(reply_mapping: Dict[str, str], all_tweet_ids: Set[str]) -> Dict[str, str]:
    """Use Union-Find to efficiently find thread roots.
    
    Returns:
        Dictionary mapping tweet_id -> thread_root_id
    """
    print("Phase 2: Finding thread roots...")
    start_time = time.time()
    
    uf = UnionFind()
    
    # Build connected components
    for child, parent in reply_mapping.items():
        uf.union(child, parent)
    
    # Map each tweet to its thread root
    tweet_to_root = {}
    for tweet_id in all_tweet_ids:
        root = uf.find(tweet_id)
        tweet_to_root[tweet_id] = root
    
    # Count threads
    roots = set(tweet_to_root.values())
    
    elapsed = time.time() - start_time
    print(f"Phase 2 complete: {len(roots):,} threads found, Time: {elapsed:.1f}s")
    
    return tweet_to_root

def stream_build_threads(file_path: str, tweet_to_root: Dict[str, str], chunk_size: int = 10000) -> Dict[str, str]:
    """Stream process JSONL again to build final threads.
    
    Returns:
        Dictionary mapping thread_root_id -> combined_text
    """
    print("Phase 3: Building thread texts...")
    start_time = time.time()
    
    thread_texts = defaultdict(list)  # root_id -> list of (timestamp, text)
    processed_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                tweet = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            tweet_id = tweet.get('tweet_id')
            tweet_text = tweet.get('full_text', '')
            created_at = tweet.get('created_at', '')
            
            if not tweet_id or is_retweet(tweet_text):
                continue
                
            # Only process tweets that belong to threads
            if tweet_id in tweet_to_root:
                root_id = tweet_to_root[tweet_id]
                thread_texts[root_id].append((created_at, tweet_text))
                processed_count += 1
            
            # For every non-root tweet, create a map that links parent -> child

            # Progress reporting
            if line_num % chunk_size == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed
                print(f"  Processed {line_num:,} lines ({rate:.0f} lines/sec), "
                      f"building {len(thread_texts):,} threads")
    
    # Sort and combine texts within each thread by timestamp
    final_threads = {}
    for root_id, texts in thread_texts.items():
        # Sort by timestamp
        texts.sort(key=lambda x: x[0])
        combined_text = " ".join(text for _, text in texts)
        final_threads[root_id] = combined_text
    
    elapsed = time.time() - start_time
    print(f"Phase 3 complete: {len(final_threads):,} threads built, "
          f"{processed_count:,} tweets processed, Time: {elapsed:.1f}s")
    
    return final_threads

def group_tweets_into_threads(jsonl_file: str, chunk_size: int = 10000) -> Dict[str, str]:
    """
    Scalable function to group tweets into threads across all accounts.
    Uses streaming processing to handle large datasets efficiently.
    
    Args:
        jsonl_file: Path to JSONL file with tweet data
        chunk_size: Number of lines to process before progress update
    
    Returns:
        Dictionary mapping thread_root_id -> combined_text
    """
    print(f"Starting scalable thread grouping for {jsonl_file}")
    print(f"Chunk size: {chunk_size:,} lines")
    
    # Phase 1: Build reply relationships with streaming
    reply_mapping, all_tweet_ids, total_tweets = stream_build_reply_relationships(jsonl_file, chunk_size)
    
    if not reply_mapping and not all_tweet_ids:
        print("No tweets found.")
        return {}
    
    # Phase 2: Find thread roots efficiently
    tweet_to_root = find_thread_roots(reply_mapping, all_tweet_ids)
    
    # Phase 3: Build final threads with streaming
    final_threads = stream_build_threads(jsonl_file, tweet_to_root, chunk_size)
    
    print(f"\nFinal result: {len(final_threads):,} threads from {total_tweets:,} total tweets")
    return final_threads

def save_threads_to_jsonl(threads: Dict[str, str], output_file: str):
    """Save threads to a JSONL file."""
    thread_data = []
    for thread_id, combined_text in threads.items():
        thread_data.append({
            'thread_id': thread_id,
            'combined_text': combined_text,
            'word_count': len(combined_text.split())
        })
    
    # Sort by word count descending
    thread_data.sort(key=lambda x: x['word_count'], reverse=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for thread in thread_data:
            f.write(json.dumps(thread, ensure_ascii=False) + '\n')
    
    print(f"Saved {len(thread_data)} threads to {output_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python group_tweets_into_threads.py <input_jsonl> [output_jsonl] [chunk_size]")
        print("Example: python group_tweets_into_threads.py posts.jsonl threads.jsonl 10000")
        print("")
        print("Arguments:")
        print("  input_jsonl   : Path to input JSONL file with tweet data")
        print("  output_jsonl  : Path to output JSONL file (default: threads.jsonl)")
        print("  chunk_size    : Lines to process between progress updates (default: 10000)")
        print("")
        print("Note: This version processes ALL accounts and builds cross-account threads.")
        print("Memory usage is optimized for large datasets (multi-GB).")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "threads.jsonl"
    chunk_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10000
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Chunk size: {chunk_size:,}")
    print("-" * 50)
    
    try:
        overall_start = time.time()
        threads = group_tweets_into_threads(input_file, chunk_size)
        
        if threads:
            save_threads_to_jsonl(threads, output_file)
            
            # Print comprehensive stats
            thread_lengths = [len(text.split()) for text in threads.values()]
            total_time = time.time() - overall_start
            
            print(f"\n{'='*50}")
            print(f"FINAL STATISTICS")
            print(f"{'='*50}")
            print(f"Total processing time: {total_time:.1f} seconds")
            print(f"Threads created: {len(threads):,}")
            print(f"Average thread length: {sum(thread_lengths) / len(thread_lengths):.1f} words")
            print(f"Longest thread: {max(thread_lengths):,} words")
            print(f"Shortest thread: {min(thread_lengths):,} words")
            print(f"Threads with 1 tweet: {sum(1 for length in thread_lengths if length <= 50):,}")
            print(f"Threads with 2+ tweets: {sum(1 for length in thread_lengths if length > 50):,}")
            print(f"Output saved to: {output_file}")
        else:
            print("No threads found.")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except MemoryError:
        print(f"Error: Insufficient memory. Try reducing chunk_size (current: {chunk_size:,}).")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()