'''
This script reads tweet data from a CSV file, generates sentence embeddings for the tweet text
using a GPU-accelerated model, and saves the output to a JSONL file for later ingestion.
'''
import pandas as pd
from sentence_transformers import SentenceTransformer
import torch
import json

# --- Configuration ---
INPUT_CSV_PATH = 'post.csv'
OUTPUT_JSONL_PATH = 'posts_with_vectors.jsonl'
MODEL_NAME = 'all-MiniLM-L6-v2' # A good starting model
TEXT_COLUMN = 'full_text' # The column containing the text to embed
BATCH_SIZE = 256 # Adjust based on your GPU's VRAM. Higher is faster.

# --- Main Execution ---
def main():
    # 1. Check for GPU availability
    if torch.cuda.is_available():
        device = 'cuda'
        print(f"GPU found: {torch.cuda.get_device_name(0)}. Using GPU.")
    else:
        device = 'cpu'
        print("No GPU found. Using CPU. This will be much slower.")

    # 2. Load the pre-trained model
    print(f"Loading model '{MODEL_NAME}'...")
    model = SentenceTransformer(MODEL_NAME, device=device)
    print("Model loaded.")

    # 3. Read the CSV file
    print(f"Reading data from {INPUT_CSV_PATH}...")
    df = pd.read_csv(INPUT_CSV_PATH)
    # Drop rows where the text column is empty, as they cannot be embedded
    df.dropna(subset=[TEXT_COLUMN], inplace=True)
    texts = df[TEXT_COLUMN].tolist()
    print(f"Found {len(texts)} tweets to process.")

    # 4. Generate embeddings in batches
    print(f"Generating embeddings with a batch size of {BATCH_SIZE}...")
    embeddings = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        device=device
    )
    print("Embeddings generated successfully.")

    # 5. Save the output to a JSONL file
    print(f"Saving results to {OUTPUT_JSONL_PATH}...")
    df['full_text_vector'] = list(embeddings)

    with open(OUTPUT_JSONL_PATH, 'w') as f:
        for record in df.to_dict(orient='records'):
            # Convert numpy array to a standard list for JSON serialization
            record['full_text_vector'] = record['full_text_vector'].tolist()
            f.write(json.dumps(record) + '\n')

    print("Processing complete.")
    print(f"Output saved to {OUTPUT_JSONL_PATH}")

if __name__ == "__main__":
    main()
