#!/usr/bin/env python3

import argparse
from pymilvus import MilvusClient, model
from typing import List, Dict, Any
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, date
from collections import defaultdict, Counter
import sys

class TwitterDateAnalyzer:
    def __init__(self, db_path: str = "data.db", collection_name: str = "twitter_posts"):
        self.client = MilvusClient(db_path)
        self.collection_name = collection_name
        self.embedding_fn = model.DefaultEmbeddingFunction()
    
    def search_with_radius(self, query: str, radius: float) -> List[Dict[str, Any]]:
        """Search for tweets within similarity radius."""
        if not self.client.has_collection(collection_name=self.collection_name):
            print(f"Error: Collection '{self.collection_name}' does not exist.")
            return []
        
        # Generate embedding for the query
        query_vectors = self.embedding_fn.encode_queries([query])
        
        # Build search parameters for radius search
        search_params = {
            "params": {
                "radius": radius
            }
        }
        
        # Search for similar vectors - use high limit since radius search returns all matches
        results = self.client.search(
            collection_name=self.collection_name,
            data=query_vectors,
            limit=10000,  # High limit to get all radius matches
            search_params=search_params,
            output_fields=["tweet_id", "full_text", "year", "month", "day"]
        )
        
        return results[0] if results else []
    
    def group_results_by_time(self, results: List[Dict[str, Any]], group_by: str) -> Dict[str, int]:
        """Group search results by time period and count occurrences."""
        time_counts = defaultdict(int)
        
        for result in results:
            year = result.get('year', 0)
            month = result.get('month', 0)  
            day = result.get('day', 0)
            
            # Skip invalid dates
            if year == 0 or month == 0 or day == 0:
                continue
            
            if group_by == 'year':
                time_key = f"{year}"
            elif group_by == 'month':
                time_key = f"{year}-{month:02d}"
            else:  # group_by == 'day'
                time_key = f"{year}-{month:02d}-{day:02d}"
            
            time_counts[time_key] += 1
        
        return dict(time_counts)
    
    def create_time_graph(self, time_counts: Dict[str, int], query: str, radius: float, 
                         group_by: str, output_file: str = None):
        """Create and display/save time series graph."""
        if not time_counts:
            print("No data to graph.")
            return
        
        # Sort time periods
        sorted_times = sorted(time_counts.keys())
        counts = [time_counts[time_period] for time_period in sorted_times]
        
        # Create figure and axis
        plt.figure(figsize=(12, 6))
        
        # Convert time strings to datetime objects for better x-axis formatting
        if group_by == 'day':
            x_values = [datetime.strptime(t, "%Y-%m-%d").date() for t in sorted_times]
            plt.plot(x_values, counts, marker='o', linewidth=2, markersize=4)
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(x_values)//10)))
        elif group_by == 'month':
            x_values = [datetime.strptime(f"{t}-01", "%Y-%m-%d").date() for t in sorted_times]
            plt.plot(x_values, counts, marker='o', linewidth=2, markersize=6)
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=max(1, len(x_values)//10)))
        else:  # year
            x_values = range(len(sorted_times))
            plt.plot(x_values, counts, marker='o', linewidth=2, markersize=8)
            plt.xticks(x_values, sorted_times)
        
        # Formatting
        plt.title(f'Timeline: "{query}" (similarity >= {radius})', fontsize=14, fontweight='bold')
        plt.xlabel(f'Time ({group_by.title()})', fontsize=12)
        plt.ylabel('Tweet Count', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Add summary info
        total_tweets = sum(counts)
        date_range = f"{sorted_times[0]} to {sorted_times[-1]}" if len(sorted_times) > 1 else sorted_times[0]
        plt.suptitle(f'Total: {total_tweets} tweets | Range: {date_range}', 
                    fontsize=10, y=0.02, alpha=0.7)
        
        # Rotate x-axis labels if needed
        if group_by in ['day', 'month']:
            plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        # Save or display
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"Graph saved to: {output_file}")
        else:
            plt.show()
    
    def print_summary(self, results: List[Dict[str, Any]], time_counts: Dict[str, int], 
                     query: str, radius: float, group_by: str):
        """Print summary statistics."""
        if not results:
            print(f"No results found for query '{query}' with similarity >= {radius}")
            return
        
        total_tweets = len(results)
        time_periods = len(time_counts)
        avg_per_period = total_tweets / time_periods if time_periods > 0 else 0
        
        # Find peak period
        peak_period = max(time_counts, key=time_counts.get) if time_counts else "N/A"
        peak_count = time_counts.get(peak_period, 0)
        
        print(f"\nSummary for query: '{query}' (similarity >= {radius})")
        print(f"Total matching tweets: {total_tweets}")
        print(f"Time periods with activity: {time_periods}")
        print(f"Average tweets per {group_by}: {avg_per_period:.1f}")
        print(f"Peak {group_by}: {peak_period} ({peak_count} tweets)")
        
        # Show similarity distribution
        similarities = [result.get('distance', 0) for result in results]
        if similarities:
            print(f"Similarity range: {min(similarities):.3f} to {max(similarities):.3f}")

def main():
    parser = argparse.ArgumentParser(description="Analyze temporal patterns in Twitter data using vector similarity")
    parser.add_argument("query", help="Search query text")
    parser.add_argument("radius", type=float, help="Minimum similarity threshold for including tweets")
    parser.add_argument("--collection", default="twitter_posts", help="Collection name (default: twitter_posts)")
    parser.add_argument("--db", default="data.db", help="Path to Milvus database file (default: data.db)")
    parser.add_argument("--group-by", choices=['day', 'month', 'year'], default='day', 
                       help="Time grouping: day, month, or year (default: day)")
    parser.add_argument("--output", help="Output file path for saving graph (PNG/PDF)")
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = TwitterDateAnalyzer(db_path=args.db, collection_name=args.collection)
    
    # Search for matching tweets
    print(f"Searching for tweets similar to '{args.query}' with similarity >= {args.radius}...")
    results = analyzer.search_with_radius(args.query, args.radius)
    
    if not results:
        print("No matching tweets found.")
        sys.exit(0)
    
    # Group results by time period
    time_counts = analyzer.group_results_by_time(results, args.group_by)
    
    # Print summary statistics
    analyzer.print_summary(results, time_counts, args.query, args.radius, args.group_by)
    
    # Generate graph
    analyzer.create_time_graph(time_counts, args.query, args.radius, args.group_by, args.output)

if __name__ == "__main__":
    main()