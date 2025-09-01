#!/usr/bin/env python3
"""
Simple script to group tweets into threads.
Only creates threads for tweets from the same user account.
"""

import json
import sys
from collections import defaultdict
from typing import Dict, Set

def is_retweet(text: str) -> bool:
    """Check if a tweet is a retweet."""
    if not text:
        return False
    text = text.strip()
    return text.startswith('RT @') or text.startswith('RT <USER_')

def group_tweets_into_threads(jsonl_file: str) -> Dict[str, dict]:
    """
    Group tweets into threads. Returns modified parent tweets with combined text.
    
    Returns:
        Dictionary mapping thread_root_id -> complete tweet data with combined text
    """
    # Store tweet info: tweet_id -> full tweet data
    tweets = {}
    # Store original tweet data for output
    original_tweets = {}
    # Pre-build replies index for O(1) lookup: tweet_id -> [list of reply tweet_ids]
    replies_to = defaultdict(list)
    
    # First pass: collect all tweet data and build replies index
    print("Reading tweets...")
    total_tweets = 0
    retweets_filtered = 0
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
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
            text = tweet.get('full_text', '')
            account_id = tweet.get('account_id')
            timestamp = tweet.get('created_at', '')
            reply_to = tweet.get('reply_to_tweet_id')
            
            if not tweet_id or not account_id:
                continue
                
            total_tweets += 1
            
            # Skip retweets
            if is_retweet(text):
                retweets_filtered += 1
                continue
            
            # Store original complete tweet data
            original_tweets[tweet_id] = tweet.copy()
            
            # Store simplified data for threading
            tweets[tweet_id] = {
                'account_id': account_id,
                'text': text,
                'timestamp': timestamp,
                'reply_to': reply_to
            }
            
            # Build replies index
            if reply_to:
                replies_to[reply_to].append(tweet_id)
    
    print(f"Processed {total_tweets:,} tweets, filtered {retweets_filtered:,} retweets")
    print(f"Kept {len(tweets):,} tweets for threading")
    
    # Build threads by following reply chains
    print("Building threads...")
    thread_roots = {}  # root_id -> combined_text
    processed = set()
    
    for tweet_id, tweet_data in tweets.items():
        if tweet_id in processed:
            continue
            
        # Find the root of this thread by going up the reply chain
        current_id = tweet_id
        seen_in_chain = set()
        
        # Follow the reply chain up to find the root
        while current_id and current_id in tweets:
            if current_id in seen_in_chain:  # Avoid infinite loops
                break
            seen_in_chain.add(current_id)
            
            current_tweet = tweets[current_id]
            
            # Check if this is a reply to another tweet from the same account
            reply_to = current_tweet['reply_to']
            if (reply_to and 
                reply_to in tweets and 
                tweets[reply_to]['account_id'] == current_tweet['account_id']):
                current_id = reply_to
            else:
                # This is the root of the thread
                break
        
        # Now collect all tweets in this thread (going down from root)
        root_id = current_id
        thread_tweets_complete = []
        
        def collect_thread_tweets(tweet_id: str, account_id: str):
            """Recursively collect all tweets in thread from this point down."""
            if tweet_id not in tweets or tweets[tweet_id]['account_id'] != account_id:
                return
            if tweet_id in processed:
                return
                
            processed.add(tweet_id)
            tweet_data = tweets[tweet_id]
            thread_tweets_complete.append((tweet_data['timestamp'], tweet_data['text']))
            
            # Use the pre-built replies index for O(1) lookup
            for reply_id in replies_to.get(tweet_id, []):
                if reply_id in tweets and tweets[reply_id]['account_id'] == account_id:
                    collect_thread_tweets(reply_id, account_id)
        
        # Collect the complete thread
        if root_id in tweets:
            collect_thread_tweets(root_id, tweets[root_id]['account_id'])
            
            if thread_tweets_complete:
                # Sort by timestamp and combine
                thread_tweets_complete.sort(key=lambda x: x[0])
                combined_text = " ".join(text for _, text in thread_tweets_complete)
                thread_roots[root_id] = combined_text
    
    # Create final output: original parent tweets with combined text
    final_tweets = {}
    for root_id, combined_text in thread_roots.items():
        if root_id in original_tweets:
            # Take the original tweet data and modify the full_text
            tweet_data = original_tweets[root_id].copy()
            tweet_data['full_text'] = combined_text
            tweet_data['thread_length'] = len(combined_text.split())
            final_tweets[root_id] = tweet_data
    
    print(f"Created {len(final_tweets):,} thread roots")
    return final_tweets

def save_threads_to_jsonl(final_tweets: Dict[str, dict], output_file: str):
    """Save modified parent tweets to JSONL file."""
    # Convert to list and sort by thread length (longest first)
    thread_data = list(final_tweets.values())
    thread_data.sort(key=lambda x: x.get('thread_length', 0), reverse=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for tweet in thread_data:
            f.write(json.dumps(tweet, ensure_ascii=False) + '\n')
    
    print(f"Saved {len(thread_data)} thread root tweets to {output_file}")
    return thread_data

def main():
    if len(sys.argv) < 2:
        print("Usage: python group_tweets_simple.py <input_jsonl> [output_jsonl]")
        print("Example: python group_tweets_simple.py posts.jsonl threads.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "threads_simple.jsonl"
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print("-" * 50)
    
    try:
        final_tweets = group_tweets_into_threads(input_file)
        
        if final_tweets:
            thread_data = save_threads_to_jsonl(final_tweets, output_file)
            
            # Print stats
            thread_lengths = [tweet.get('thread_length', 0) for tweet in thread_data]
            print(f"\nStatistics:")
            print(f"Total thread root tweets: {len(final_tweets):,}")
            print(f"Average thread length: {sum(thread_lengths) / len(thread_lengths):.1f} words")
            print(f"Longest thread: {max(thread_lengths):,} words")
            print(f"Single-tweet threads: {sum(1 for length in thread_lengths if length <= 50):,}")
            print(f"Multi-tweet threads: {sum(1 for length in thread_lengths if length > 50):,}")
        else:
            print("No threads found.")
            
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
