This is a relatively simple setup of a single opensearch node that can be used to run semantic searches across a bunch of tweets. Because OpenSearch's GPU acceleration is still a WIP as of the time of writing, it uses a separate python script to generate embeddings. Right now the script is hardcoded to run on the post.csv file available at https://huggingface.co/datasets/CommunityArchive/CommunityArchive/tree/main

Setup:  
```
docker-compose up -d 
./setup_opensearch_model.sh
python generate_embeddings.py
bun run ingest_jsonl.ts posts_with_vectors.jsonl
bun run neural_search.ts "memetics"
```