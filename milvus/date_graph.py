#!/usr/bin/env python3

import argparse
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Any
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, date
from collections import defaultdict, Counter
import sys
import cmd
import shlex

class TwitterDateAnalyzer:
    def __init__(self, db_path: str = "data.db", collection_name: str = "twitter_posts"):
        self.client = MilvusClient(db_path)
        self.collection_name = collection_name
        self.embedding_fn = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Load collection at startup
        self.load_collection()
    
    def load_collection(self):
        """Load the collection to ensure it's ready for search operations."""
        if not self.client.has_collection(collection_name=self.collection_name):
            print(f"Error: Collection '{self.collection_name}' does not exist.")
            return False
            
        try:
            self.client.load_collection(collection_name=self.collection_name)
            print(f"Collection '{self.collection_name}' loaded successfully.")
            return True
        except Exception as e:
            print(f"Warning: Could not load collection '{self.collection_name}': {e}")
            return True  # Continue anyway, might already be loaded
    
    def get_tweet_by_record_id(self, record_id: str) -> Dict[str, Any]:
        """Lookup a tweet by its record ID to get full text."""
        try:
            results = self.client.get(
                collection_name=self.collection_name,
                ids=[record_id],
                output_fields=["tweet_id", "full_text", "year", "month", "day"]
            )
            return results[0] if results else {}
        except Exception as e:
            print(f"Error looking up record {record_id}: {e}")
            return {}
    
    def search_with_radius_range(self, query: str, min_radius: float, max_radius: float) -> List[Dict[str, Any]]:
        """Search for tweets within a specific similarity range."""
        if not self.client.has_collection(collection_name=self.collection_name):
            print(f"Error: Collection '{self.collection_name}' does not exist.")
            return []
        
        # Generate embedding for the query
        query_vectors = [self.embedding_fn.encode([query])[0]]
        
        # Build search parameters for range search
        search_params = {
            "params": {
                "radius": min_radius,
                "range_filter": max_radius  # This defines the upper bound
            }
        }
        
        # Search for similar vectors in the specified range
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                data=query_vectors,
                limit=10000,  # High limit to get all matches in this range
                search_params=search_params,
                output_fields=["tweet_id", "full_text", "year", "month", "day"]
            )
            return results[0] if results else []
        except Exception as e:
            print(f"Error searching range [{min_radius:.3f}, {max_radius:.3f}]: {e}")
            return []
    
    def search_range_recursive(self, query: str, min_radius: float, max_radius: float, 
                              depth: int = 0, max_depth: int = 10) -> List[Dict[str, Any]]:
        """Recursively search a range, splitting if we hit the 10k limit."""
        if depth > max_depth:
            print(f"    {'  ' * depth}Max depth reached at [{min_radius:.6f}, {max_radius:.6f}), stopping")
            return []
        
        indent = '  ' * (depth + 1)
        range_size = max_radius - min_radius
        print(f"{indent}Searching [{min_radius:.6f}, {max_radius:.6f}) (size: {range_size:.6f})")
        
        # Search this range
        results = self.search_with_radius_range(query, min_radius, max_radius)
        
        # If we hit the 10k limit and the range is splittable, split it
        if len(results) == 10000 and range_size > 1e-6:  # Don't split tiny ranges
            print(f"{indent}Hit 10k limit, splitting range...")
            mid_radius = (min_radius + max_radius) / 2
            
            # Recursively search both halves
            left_results = self.search_range_recursive(query, min_radius, mid_radius, depth + 1, max_depth)
            right_results = self.search_range_recursive(query, mid_radius, max_radius, depth + 1, max_depth)
            
            # Combine results
            all_results = left_results + right_results
            print(f"{indent}Split complete: {len(left_results)} + {len(right_results)} = {len(all_results)} results")
            return all_results
        else:
            print(f"{indent}Found {len(results)} results (complete)")
            return results

    def search_with_radius_comprehensive(self, query: str, min_radius: float, 
                                       range_step: float = 0.05) -> List[Dict[str, Any]]:
        """Search systematically through similarity ranges to get all matches."""
        all_results = []
        seen_record_ids = set()
        current_radius = min_radius
        max_similarity = 1.0  # Cosine similarity max value
        example_record_ids = []  # Collect one record ID from each range
        
        print(f"Starting comprehensive search from radius {min_radius} with step {range_step}")
        
        range_count = 0
        while current_radius < max_similarity:
            range_end = min(current_radius + range_step, max_similarity)
            range_count += 1
            
            print(f"Range {range_count}: [{current_radius:.3f}, {range_end:.3f})")
            
            # Use recursive search to handle ranges that exceed 10k limit
            range_results = self.search_range_recursive(query, current_radius, range_end)
            
            # Collect an example record ID and similarity from this range
            if range_results:
                example_record_ids.append({
                    'record_id': range_results[0].get('id', ''),
                    'similarity': range_results[0].get('distance', 0)
                })

            # Deduplicate by id (primary key)
            new_results = []
            for result in range_results:
                record_id = result.get('id', None)
                if record_id is not None and record_id not in seen_record_ids:
                    seen_record_ids.add(record_id)
                    new_results.append(result)
            
            all_results.extend(new_results)
            print(f"  Total: {len(range_results)} results ({len(new_results)} new, {len(range_results) - len(new_results)} duplicates)")
            
            current_radius = range_end
        
        total_unique = len(all_results)
        print(f"Comprehensive search complete: {total_unique} unique results across {range_count} ranges")
        
        # Print example tweets from each range
        if example_record_ids:
            print(f"\nExample tweets from each range:")
            for i, example in enumerate(example_record_ids):
                if isinstance(example, dict) and example.get('record_id'):
                    record_id = example['record_id']
                    similarity = example['similarity']
                    
                    tweet_data = self.get_tweet_by_record_id(record_id)
                    tweet_text = tweet_data.get('full_text', 'Unable to retrieve text')
                    
                    # Truncate to 300 chars for readability
                    if len(tweet_text) > 300:
                        tweet_text = tweet_text[:300] + "..."
                    
                    print(f"  [{similarity:.3f}]: {tweet_text}")
                else:
                    print(f"  Range {i+1}: No record ID available")
        
        return all_results
    
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

class TwitterCLI(cmd.Cmd):
    """Interactive CLI for Twitter date analysis."""
    intro = """
Twitter Date Analysis CLI
=========================
Database loaded and ready. Type 'help' for available commands.
Type 'help <command>' for detailed help on a specific command.
"""
    prompt = 'twitter> '
    
    def __init__(self, db_path: str = "data.db", collection_name: str = "twitter_posts"):
        super().__init__()
        self.analyzer = TwitterDateAnalyzer(db_path, collection_name)
        print(f"Initialized with database: {db_path}, collection: {collection_name}")
        
    def do_search(self, line):
        """Search for tweets and generate timeline graph.
        Usage: search <query> <min_similarity> [--group-by day|month|year] [--output filename] [--range-step 0.05] [--debug]
        
        Examples:
          search politics 0.3
          search "climate change" 0.4 --group-by month --output climate.png
          search english 0.0 --range-step 0.02 --debug
        """
        try:
            args = shlex.split(line)
            if len(args) < 2:
                print("Error: Need at least query and min_similarity")
                print("Usage: search <query> <min_similarity> [options]")
                return
                
            query = args[0]
            min_similarity = float(args[1])
            
            # Parse optional arguments
            group_by = 'day'
            output_file = None
            range_step = 0.05
            debug = False
            
            i = 2
            while i < len(args):
                if args[i] == '--group-by' and i + 1 < len(args):
                    group_by = args[i + 1]
                    i += 2
                elif args[i] == '--output' and i + 1 < len(args):
                    output_file = args[i + 1]
                    i += 2
                elif args[i] == '--range-step' and i + 1 < len(args):
                    range_step = float(args[i + 1])
                    i += 2
                elif args[i] == '--debug':
                    debug = True
                    i += 1
                else:
                    print(f"Unknown argument: {args[i]}")
                    return
            
            # Validate arguments
            if group_by not in ['day', 'month', 'year']:
                print("Error: group-by must be 'day', 'month', or 'year'")
                return
                
            # Perform search
            print(f"Searching for tweets similar to '{query}' with similarity >= {min_similarity}...")
            results = self.analyzer.search_with_radius_comprehensive(query, min_similarity, range_step)
            
            if not results:
                print("No matching tweets found.")
                return
                
            # Debug output
            if debug:
                print(f"\nDEBUG: Raw results count: {len(results)}")
                
                # Check for duplicate tweet_ids
                tweet_ids = [r.get('tweet_id', '') for r in results]
                unique_tweet_ids = set(tweet_ids)
                print(f"DEBUG: Unique tweet IDs: {len(unique_tweet_ids)}")
                if len(tweet_ids) != len(unique_tweet_ids):
                    print(f"DEBUG: WARNING - Found {len(tweet_ids) - len(unique_tweet_ids)} duplicate tweet IDs!")
                
                # Check similarity range
                similarities = [r.get('distance', 0) for r in results]
                if similarities:
                    print(f"DEBUG: Actual similarity range: {min(similarities):.3f} to {max(similarities):.3f}")
                    print(f"DEBUG: Results below threshold: {sum(1 for s in similarities if s < min_similarity)}")
                
                # Check date distribution before grouping
                dates = []
                for r in results:
                    year, month, day = r.get('year', 0), r.get('month', 0), r.get('day', 0)
                    if year > 0 and month > 0 and day > 0:
                        dates.append(f"{year}-{month:02d}-{day:02d}")
                        
                date_counts_raw = Counter(dates)
                print(f"DEBUG: Top 5 dates before grouping:")
                for date_str, count in date_counts_raw.most_common(5):
                    print(f"  {date_str}: {count}")
                
            # Group results by time period
            time_counts = self.analyzer.group_results_by_time(results, group_by)
            
            if debug:
                print(f"DEBUG: After grouping - total count: {sum(time_counts.values())}")
                print(f"DEBUG: Top 5 dates after grouping:")
                for date_str, count in sorted(time_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {date_str}: {count}")
            
            # Print summary statistics
            self.analyzer.print_summary(results, time_counts, query, min_similarity, group_by)
            
            # Generate graph
            self.analyzer.create_time_graph(time_counts, query, min_similarity, group_by, output_file)
            
        except ValueError as e:
            print(f"Error: Invalid number format - {e}")
        except Exception as e:
            print(f"Error: {e}")
    
    def do_status(self, line):
        """Show database and collection status."""
        print(f"Database: {self.analyzer.client}")
        print(f"Collection: {self.analyzer.collection_name}")
        
        if self.analyzer.client.has_collection(self.analyzer.collection_name):
            try:
                # Get collection info
                collection_info = self.analyzer.client.describe_collection(self.analyzer.collection_name)
                print(f"Collection exists and is loaded")
                print(f"Schema: {collection_info}")
            except Exception as e:
                print(f"Collection exists but error getting info: {e}")
        else:
            print("Collection does not exist")
    
    def do_collections(self, line):
        """List all collections in the database."""
        try:
            collections = self.analyzer.client.list_collections()
            print("Available collections:")
            for collection in collections:
                print(f"  - {collection}")
        except Exception as e:
            print(f"Error listing collections: {e}")
            
    def do_use(self, line):
        """Switch to a different collection.
        Usage: use <collection_name>
        """
        if not line.strip():
            print("Error: Need collection name")
            print("Usage: use <collection_name>")
            return
            
        collection_name = line.strip()
        if self.analyzer.client.has_collection(collection_name):
            self.analyzer.collection_name = collection_name
            self.analyzer.load_collection()
            print(f"Switched to collection: {collection_name}")
        else:
            print(f"Error: Collection '{collection_name}' does not exist")
    
    def do_quit(self, line):
        """Exit the CLI."""
        print("Goodbye!")
        return True
        
    def do_exit(self, line):
        """Exit the CLI."""
        return self.do_quit(line)
        
    def do_EOF(self, line):
        """Handle Ctrl+D to exit."""
        print("\nGoodbye!")
        return True
        
    def emptyline(self):
        """Do nothing on empty line."""
        pass

def main():
    parser = argparse.ArgumentParser(description="Analyze temporal patterns in Twitter data using vector similarity")
    parser.add_argument("--collection", default="twitter_posts", help="Collection name (default: twitter_posts)")
    parser.add_argument("--db", default="data.db", help="Path to Milvus database file (default: data.db)")
    parser.add_argument("--interactive", "-i", action="store_true", help="Start interactive CLI mode")
    
    # Optional positional arguments for one-shot mode
    parser.add_argument("query", nargs='?', help="Search query text (for one-shot mode)")
    parser.add_argument("radius", nargs='?', type=float, help="Minimum similarity threshold (for one-shot mode)")
    parser.add_argument("--group-by", choices=['day', 'month', 'year'], default='day', 
                       help="Time grouping: day, month, or year (default: day)")
    parser.add_argument("--output", help="Output file path for saving graph (PNG/PDF)")
    parser.add_argument("--range-step", type=float, default=0.05,
                       help="Step size for similarity range sweeping (default: 0.05)")
    
    args = parser.parse_args()
    
    # If interactive mode or no query provided, start CLI
    if args.interactive or not args.query or args.radius is None:
        print("Starting interactive mode...")
        cli = TwitterCLI(db_path=args.db, collection_name=args.collection)
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print("\nGoodbye!")
        return
    
    # One-shot mode (original behavior)
    print("Running in one-shot mode...")
    
    # Initialize analyzer
    analyzer = TwitterDateAnalyzer(db_path=args.db, collection_name=args.collection)
    
    # Search for matching tweets using comprehensive range-based approach
    print(f"Searching for tweets similar to '{args.query}' with similarity >= {args.radius}...")
    results = analyzer.search_with_radius_comprehensive(args.query, args.radius, args.range_step)
    
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