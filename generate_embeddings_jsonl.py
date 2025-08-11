'''
This script reads tweet data from a JSONL file, generates sentence embeddings for the tweet text
using a GPU-accelerated model, and saves the output to a JSONL file for later ingestion.
'''
import json
from sentence_transformers import SentenceTransformer
import torch

# --- Configuration ---
INPUT_JSONL_PATH = 'post.jsonl'
OUTPUT_JSONL_PATH = 'posts_with_vectors.jsonl'
MODEL_NAME = 'all-MiniLM-L6-v2' # A good starting model
TEXT_COLUMN = 'full_text' # The field containing the text to embed
# Adjust based on your GPU's VRAM and the nature of your data.
# Larger batches are faster but use more memory.
BATCH_SIZE = 256 

# --- Main Execution ---
def main():
    print(f"Starting embedding generation...")
    print(f"Input JSONL: {INPUT_JSONL_PATH}")
    print(f"Output JSONL: {OUTPUT_JSONL_PATH}")
    print(f"Model: {MODEL_NAME}")
    print(f"Text field: {TEXT_COLUMN}")
    print(f"Batch size: {BATCH_SIZE}")
    
    # 1. Check for GPU availability
    if torch.cuda.is_available():
        device = 'cuda'
        print(f"GPU found: {torch.cuda.get_device_name(0)}. Using GPU.")
        print(f"GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")
    else:
        device = 'cpu'
        print("No GPU found. Using CPU. This will be much slower.")

    # 2. Load the pre-trained model
    print(f"Loading model '{MODEL_NAME}'...")
    try:
        model = SentenceTransformer(MODEL_NAME, device=device)
        print("Model loaded successfully.")
        print(f"Model max sequence length: {model.max_seq_length}")
        print(f"Model embedding dimension: {model.get_sentence_embedding_dimension()}")
    except Exception as e:
        print(f"Error loading model: {e}")
        raise

    # 3. Process JSONL in chunks and write to JSONL
    print(f"Processing {INPUT_JSONL_PATH} in batches and writing to {OUTPUT_JSONL_PATH}...")
    
    try:
        with open(INPUT_JSONL_PATH, 'r', encoding='utf-8') as jsonlfile, \
             open(OUTPUT_JSONL_PATH, 'w') as output_jsonlfile:
            
            batch = []
            total_rows = 0
            processed_rows = 0

            for line in jsonlfile:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    row = json.loads(line)
                    total_rows += 1
                    
                    # Debug first few rows
                    if total_rows <= 3:
                        print(f"Row {total_rows}: {row}")
                    
                    # Only process rows that have content in the text field
                    text_content = row.get(TEXT_COLUMN, '').strip() if isinstance(row.get(TEXT_COLUMN), str) else str(row.get(TEXT_COLUMN, '')).strip()
                    if text_content:
                        # Validate text length
                        if len(text_content) > 10000:  # Arbitrary long text threshold
                            print(f"Warning: Very long text in row {total_rows} ({len(text_content)} chars), truncating...")
                            row[TEXT_COLUMN] = text_content[:10000]
                        batch.append(row)
                    else:
                        if total_rows <= 10:  # Log first few missing texts
                            print(f"Warning: No text content in row {total_rows}")
                    
                    if len(batch) >= BATCH_SIZE:
                        try:
                            process_batch(batch, model, output_jsonlfile, TEXT_COLUMN, device)
                            processed_rows += len(batch)
                            print(f"Processed {processed_rows} of {total_rows} rows...")
                            batch = []
                        except Exception as e:
                            print(f"Error processing batch at row {total_rows}: {e}")
                            print(f"Batch size: {len(batch)}")
                            # Log problematic batch data
                            for i, problematic_row in enumerate(batch):
                                text_len = len(str(problematic_row.get(TEXT_COLUMN, '')))
                                print(f"  Batch item {i}: text length {text_len}")
                            raise

                except json.JSONDecodeError as e:
                    print(f"Warning: Invalid JSON on line {total_rows + 1}: {e}")
                    continue

            # Process the final batch
            if batch:
                try:
                    process_batch(batch, model, output_jsonlfile, TEXT_COLUMN, device)
                    processed_rows += len(batch)
                    print(f"Processed final batch: {len(batch)} rows")
                except Exception as e:
                    print(f"Error processing final batch: {e}")
                    raise

        print("\nProcessing complete.")
        print(f"Total rows read: {total_rows}")
        print(f"Rows with text processed: {processed_rows}")
        print(f"Output saved to {OUTPUT_JSONL_PATH}")

    except FileNotFoundError as e:
        print(f"Error: Input file not found at {INPUT_JSONL_PATH}")
        print(f"FileNotFoundError details: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise


def process_batch(batch, model, jsonlfile, text_column, device):
    """
    Generates embeddings for a batch of records and writes them to the JSONL file.
    """
    try:
        texts = [str(row[text_column]) for row in batch]
        print(f"Processing batch of {len(texts)} texts...")
        
        # Validate texts
        for i, text in enumerate(texts):
            if not isinstance(text, str):
                print(f"Warning: Non-string text at index {i}: {type(text)} - {text}")
                texts[i] = str(text)
            elif len(text.strip()) == 0:
                print(f"Warning: Empty text at index {i}")
        
        print(f"Generating embeddings...")
        embeddings = model.encode(
            texts,
            show_progress_bar=False, # Progress is shown by row count in main loop
            device=device,
            batch_size=min(32, len(texts))  # Smaller sub-batches to avoid memory issues
        )
        print(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")

        # Add embeddings to the records and write to file
        for i, record in enumerate(batch):
            try:
                embedding_list = embeddings[i].tolist()
                record['full_text_vector'] = embedding_list
                json_line = json.dumps(record)
                jsonlfile.write(json_line + '\n')
            except Exception as e:
                print(f"Error processing record {i}: {e}")
                print(f"Record: {record}")
                print(f"Embedding shape: {embeddings[i].shape if i < len(embeddings) else 'N/A'}")
                raise
                
    except Exception as e:
        print(f"Error in process_batch: {e}")
        print(f"Batch size: {len(batch)}")
        print(f"Device: {device}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()