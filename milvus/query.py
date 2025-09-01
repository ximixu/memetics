#!/usr/bin/env python3

import argparse
from pymilvus import MilvusClient, model
from typing import List, Dict, Any

class TwitterQueryEngine:
    def __init__(self, db_path: str = "data.db", collection_name: str = "twitter_posts"):
        self.client = MilvusClient(db_path)
        self.collection_name = collection_name
        self.embedding_fn = model.DefaultEmbeddingFunction()
    
    def search(self, query: str, limit: int = 10, radius: float = None) -> List[Dict[str, Any]]:
        """Search for similar tweets using vector similarity."""
        if not self.client.has_collection(collection_name=self.collection_name):
            print(f"Error: Collection '{self.collection_name}' does not exist.")
            return []
        
        # Generate embedding for the query
        query_vectors = self.embedding_fn.encode_queries([query])
        
        # Build search parameters
        search_params = {}
        if radius is not None:
            search_params = {
                "params": {
                    "radius": radius
                }
            }
        
        # Search for similar vectors
        results = self.client.search(
            collection_name=self.collection_name,
            data=query_vectors,
            limit=limit,
            search_params=search_params if search_params else None,
            output_fields=["tweet_id", "full_text", "year", "month", "day"]
        )
        
        return results[0] if results else []
    
    def display_results(self, results: List[Dict[str, Any]], query: str, radius: float = None):
        """Display search results in a readable format."""
        print(f"\nQuery: '{query}'")
        if radius is not None:
            print(f"Minimum similarity: {radius} (returning ALL results with similarity >= {radius})")
        print(f"Found {len(results)} tweets:\n")
        
        for i, result in enumerate(results, 1):
            similarity = result.get('distance', 0)  # Milvus returns cosine similarity as "distance"
            
            print(f"{i}. [Similarity: {similarity:.3f}]")
            print(f"   Date: {result['year']}-{result['month']:02d}-{result['day']:02d}")
            print(f"   Tweet ID: {result['tweet_id']}")
            print(f"   Text: {result['full_text']}")
            print()

def main():
    parser = argparse.ArgumentParser(description="Query Twitter data using vector similarity search")
    parser.add_argument("query", help="Search query text")
    parser.add_argument("--collection", default="twitter_posts", help="Collection name (default: twitter_posts)")
    parser.add_argument("--db", default="data.db", help="Path to Milvus database file (default: data.db)")
    parser.add_argument("--limit", type=int, default=10, help="Number of results to return (default: 10)")
    parser.add_argument("--radius", type=float, help="Minimum similarity threshold - returns ALL results with similarity >= this value (higher values = more similar)")
    
    args = parser.parse_args()
    
    # Initialize query engine
    engine = TwitterQueryEngine(db_path=args.db, collection_name=args.collection)
    
    # Search for similar tweets
    results = engine.search(args.query, limit=args.limit, radius=args.radius)
    
    # Display results
    if results:
        engine.display_results(results, args.query, args.radius)
    else:
        radius_info = f" within radius {args.radius}" if args.radius else ""
        print(f"No results found for query: '{args.query}'{radius_info}")

if __name__ == "__main__":
    main()