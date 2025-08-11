#!/usr/bin/env python3
"""
Search interface for posts using FAISS semantic search and SQLite FTS.
Supports monthly frequency and semantic cluster queries.
"""

import sqlite3
import numpy as np
import faiss
from typing import List, Dict, Any, Tuple, Optional
import argparse
from pathlib import Path
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logger.warning("sentence-transformers not available. Install with: pip install sentence-transformers")

class PostSearcher:
    def __init__(self, db_path: str = "posts.db", faiss_index_path: str = "posts_faiss.index"):
        self.db_path = db_path
        self.faiss_index_path = faiss_index_path
        self.db_conn = None
        self.faiss_index = None
        self.embedding_model = None
        
    def load_indices(self):
        """Load SQLite database and FAISS index."""
        # Load SQLite
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}")
        
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.row_factory = sqlite3.Row  # Enable column access by name
        
        # Load FAISS index
        if Path(self.faiss_index_path).exists():
            self.faiss_index = faiss.read_index(self.faiss_index_path)
            logger.info(f"Loaded FAISS index with {self.faiss_index.ntotal} vectors, dimension: {self.faiss_index.d}")
        else:
            logger.warning(f"FAISS index not found: {self.faiss_index_path}")
    
    def load_embedding_model(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        """Load the sentence transformer model for text embedding."""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError("sentence-transformers not available. Install with: pip install sentence-transformers")
        
        if self.embedding_model is None:
            logger.info(f"Loading embedding model: {model_name}")
            self.embedding_model = SentenceTransformer(model_name)
        return self.embedding_model
    
    def embed_text(self, text: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> np.ndarray:
        """Convert text to embedding vector."""
        model = self.load_embedding_model(model_name)
        embedding = model.encode([text])
        return embedding[0].astype(np.float32)
    
    def full_text_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search posts using SQLite FTS."""
        cursor = self.db_conn.cursor()
        
        # FTS query with match operator
        cursor.execute('''
            SELECT p.*, rank 
            FROM posts_fts 
            JOIN posts p ON posts_fts.rowid = p.id 
            WHERE posts_fts MATCH ? 
            ORDER BY rank 
            LIMIT ?
        ''', (query, limit))
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        return results
    
    def semantic_search(self, query_vector: np.ndarray, k: int = 10) -> List[Tuple[int, float]]:
        """Search posts using FAISS semantic similarity."""
        if self.faiss_index is None:
            logger.error("FAISS index not loaded")
            return []
        
        # Normalize query vector for cosine similarity
        query_vector = query_vector.astype(np.float32)
        norm = np.linalg.norm(query_vector)
        if norm > 0:
            query_vector = query_vector / norm
        
        # Reshape for FAISS
        query_vector = query_vector.reshape(1, -1)
        
        # Search
        scores, indices = self.faiss_index.search(query_vector, k)
        return list(zip(indices[0], scores[0]))
    
    def semantic_search_text(self, query_text: str, k: int = 10, model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> List[Dict[str, Any]]:
        """Search posts using semantic similarity from text query."""
        # Convert text to embedding
        query_vector = self.embed_text(query_text, model_name)
        
        # Perform semantic search
        search_results = self.semantic_search(query_vector, k)
        
        if not search_results:
            return []
        
        # Get the post indices and scores
        indices = [idx for idx, _ in search_results]
        scores = [score for _, score in search_results]
        
        # Get the actual posts
        posts = self.get_posts_by_indices(indices)
        
        # Add similarity scores to posts
        for i, post in enumerate(posts):
            if i < len(scores):
                post['similarity_score'] = float(scores[i])
        
        return posts
    
    def get_posts_by_indices(self, indices: List[int]) -> List[Dict[str, Any]]:
        """Get posts by their sequential indices (FAISS row order)."""
        cursor = self.db_conn.cursor()
        
        # Get all posts ordered by ID to match FAISS index order
        cursor.execute('SELECT * FROM posts ORDER BY id')
        all_posts = cursor.fetchall()
        
        # Return posts at specific indices
        results = []
        for idx in indices:
            if 0 <= idx < len(all_posts):
                results.append(dict(all_posts[idx]))
            else:
                logger.warning(f"Index {idx} out of range (total posts: {len(all_posts)})")
        
        return results
    
    def monthly_frequency_analysis(self, 
                                 start_date: Optional[str] = None, 
                                 end_date: Optional[str] = None,
                                 topic_filter: Optional[str] = None) -> Dict[str, Any]:
        """Analyze post frequency by month."""
        cursor = self.db_conn.cursor()
        
        query = '''
            SELECT 
                strftime('%Y-%m', created_at) as month,
                topic,
                COUNT(*) as count,
                AVG(CAST(favorite_count as REAL)) as avg_favorites,
                AVG(CAST(retweet_count as REAL)) as avg_retweets
            FROM posts 
            WHERE created_at IS NOT NULL
        '''
        params = []
        
        if start_date:
            query += ' AND created_at >= ?'
            params.append(start_date)
            
        if end_date:
            query += ' AND created_at <= ?'
            params.append(end_date)
            
        if topic_filter:
            query += ' AND topic = ?'
            params.append(topic_filter)
        
        query += '''
            GROUP BY strftime('%Y-%m', created_at), topic
            ORDER BY month, topic
        '''
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Organize results
        monthly_data = defaultdict(lambda: defaultdict(dict))
        for row in results:
            month, topic, count, avg_fav, avg_rt = row
            monthly_data[month][topic or 'Unknown'] = {
                'count': count,
                'avg_favorites': round(avg_fav or 0, 2),
                'avg_retweets': round(avg_rt or 0, 2)
            }
        
        return dict(monthly_data)
    
    def topic_distribution(self) -> Dict[str, int]:
        """Get distribution of posts by topic."""
        cursor = self.db_conn.cursor()
        cursor.execute('SELECT topic, COUNT(*) FROM posts GROUP BY topic ORDER BY COUNT(*) DESC')
        
        return {topic or 'Unknown': count for topic, count in cursor.fetchall()}
    
    def semantic_clustering_analysis(self, 
                                   sample_size: int = 1000, 
                                   n_clusters: int = 10) -> Dict[str, Any]:
        """Perform basic semantic clustering analysis using FAISS vectors."""
        if self.faiss_index is None:
            logger.error("FAISS index not loaded")
            return {}
        
        try:
            # Get sample of vectors
            total_vectors = self.faiss_index.ntotal
            if sample_size > total_vectors:
                sample_size = total_vectors
            
            # Get all vectors from FAISS index
            vectors = self.faiss_index.reconstruct_n(0, sample_size)
            
            # Simple k-means clustering using FAISS
            kmeans = faiss.Kmeans(vectors.shape[1], n_clusters, niter=20, verbose=False)
            kmeans.train(vectors)
            
            # Get cluster assignments
            _, cluster_assignments = kmeans.index.search(vectors, 1)
            cluster_assignments = cluster_assignments.flatten()
            
            # Get posts for analysis
            cursor = self.db_conn.cursor()
            cursor.execute('SELECT id, full_text, topic FROM posts ORDER BY id LIMIT ?', (sample_size,))
            posts = cursor.fetchall()
            
            # Organize by clusters
            clusters = defaultdict(list)
            for i, (post_id, text, topic) in enumerate(posts):
                if i < len(cluster_assignments):
                    cluster_id = int(cluster_assignments[i])
                    clusters[cluster_id].append({
                        'id': post_id,
                        'text': text[:200] + '...' if len(text) > 200 else text,
                        'topic': topic
                    })
            
            # Analyze clusters
            cluster_analysis = {}
            for cluster_id, cluster_posts in clusters.items():
                topics = [p['topic'] for p in cluster_posts if p['topic']]
                topic_counts = Counter(topics)
                
                cluster_analysis[f"cluster_{cluster_id}"] = {
                    'size': len(cluster_posts),
                    'top_topics': dict(topic_counts.most_common(3)),
                    'sample_posts': cluster_posts[:3]  # Show first 3 as examples
                }
            
            return {
                'total_posts_analyzed': sample_size,
                'n_clusters': n_clusters,
                'clusters': cluster_analysis
            }
            
        except Exception as e:
            logger.error(f"Error in clustering analysis: {e}")
            return {}
    
    def close(self):
        """Close database connection."""
        if self.db_conn:
            self.db_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Search posts using FTS and semantic search")
    parser.add_argument("--db", default="posts.db", help="SQLite database path")
    parser.add_argument("--faiss-index", default="posts_faiss.index", help="FAISS index path")
    
    subparsers = parser.add_subparsers(dest='command', help='Search commands')
    
    # FTS search
    fts_parser = subparsers.add_parser('fts', help='Full-text search')
    fts_parser.add_argument('query', help='Search query')
    fts_parser.add_argument('--limit', type=int, default=10, help='Number of results')
    
    # Monthly frequency analysis
    freq_parser = subparsers.add_parser('frequency', help='Monthly frequency analysis')
    freq_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    freq_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    freq_parser.add_argument('--topic', help='Filter by topic')
    
    # Topic distribution
    topic_parser = subparsers.add_parser('topics', help='Topic distribution')
    
    # Semantic search
    semantic_parser = subparsers.add_parser('semantic', help='Semantic search using embeddings')
    semantic_parser.add_argument('query', help='Search query text')
    semantic_parser.add_argument('--limit', type=int, default=10, help='Number of results')
    semantic_parser.add_argument('--embedding-model', default='sentence-transformers/all-MiniLM-L6-v2', help='Embedding model to use')
    
    # Clustering analysis
    cluster_parser = subparsers.add_parser('cluster', help='Semantic clustering analysis')
    cluster_parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for clustering')
    cluster_parser.add_argument('--n-clusters', type=int, default=10, help='Number of clusters')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    searcher = PostSearcher(args.db, args.faiss_index)
    try:
        searcher.load_indices()
        
        if args.command == 'fts':
            results = searcher.full_text_search(args.query, args.limit)
            print(f"\nFound {len(results)} results for '{args.query}':")
            for i, post in enumerate(results, 1):
                print(f"\n{i}. {post['username']} ({post['created_at']})")
                print(f"   Topic: {post['topic']}")
                print(f"   Text: {post['full_text']}")
                print(f"   Engagement: {post['favorite_count']} likes, {post['retweet_count']} retweets")
        
        elif args.command == 'frequency':
            results = searcher.monthly_frequency_analysis(
                args.start_date, args.end_date, args.topic
            )
            print("\nMonthly frequency analysis:")
            print(json.dumps(results, indent=2))
        
        elif args.command == 'semantic':
            try:
                results = searcher.semantic_search_text(args.query, args.limit, args.embedding_model)
                print(f"\nFound {len(results)} semantic results for '{args.query}':")
                for i, post in enumerate(results, 1):
                    similarity = post.get('similarity_score', 0)
                    print(f"\n{i}. {post['username']} ({post['created_at']}) - Similarity: {similarity:.4f}")
                    print(f"   Topic: {post['topic']}")
                    print(f"   Text: {post['full_text']}")
                    print(f"   Engagement: {post['favorite_count']} likes, {post['retweet_count']} retweets")
            except ImportError as e:
                print(f"Error: {e}")
                print("Please install sentence-transformers: pip install sentence-transformers")
            except Exception as e:
                print(f"Error performing semantic search: {e}")
                import traceback
                traceback.print_exc()
        
        elif args.command == 'topics':
            results = searcher.topic_distribution()
            print("\nTopic distribution:")
            for topic, count in results.items():
                print(f"  {topic}: {count}")
        
        elif args.command == 'cluster':
            results = searcher.semantic_clustering_analysis(
                args.sample_size, args.n_clusters
            )
            print("\nSemantic clustering analysis:")
            print(json.dumps(results, indent=2))
    
    finally:
        searcher.close()


if __name__ == "__main__":
    main()