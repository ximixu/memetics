#!/bin/bash

# OpenSearch all-MiniLM-L6-v2 Model Setup Script
# This script downloads and registers the all-MiniLM-L6-v2 model for neural search

set -e

OPENSEARCH_HOST="localhost:9200"
MODEL_NAME="all-MiniLM-L6-v2"
MODEL_URL="https://artifacts.opensearch.org/models/ml-models/huggingface/sentence-transformers/all-MiniLM-L6-v2/1.0.2/torch_script/sentence-transformers_all-MiniLM-L6-v2-1.0.2-torch_script.zip"

echo "🚀 Starting OpenSearch all-MiniLM-L6-v2 model setup..."

# Check if OpenSearch is running
echo "📡 Checking OpenSearch connection..."
if ! curl -s "http://${OPENSEARCH_HOST}/_cat/health" > /dev/null; then
    echo "❌ Error: OpenSearch is not running or not accessible at ${OPENSEARCH_HOST}"
    echo "💡 Please start OpenSearch first: docker-compose up -d opensearch"
    exit 1
fi

echo "✅ OpenSearch is running"

# Create model group first
echo "📁 Creating model group..."
MODEL_GROUP_RESPONSE=$(curl -s -X POST "http://${OPENSEARCH_HOST}/_plugins/_ml/model_groups/_register" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "local_model_group",
        "description": "Local model group for text embedding models"
    }')

MODEL_GROUP_ID=$(echo $MODEL_GROUP_RESPONSE | grep -o '"model_group_id":"[^"]*' | cut -d'"' -f4)
echo "✅ Model group created with ID: ${MODEL_GROUP_ID}"

# Register the pretrained model
echo "📦 Registering huggingface/sentence-transformers/all-MiniLM-L6-v2 model..."
REGISTER_RESPONSE=$(curl -s -X POST "http://${OPENSEARCH_HOST}/_plugins/_ml/models/_register" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "huggingface/sentence-transformers/all-MiniLM-L6-v2",
        "version": "1.0.2",
        "description": "all-MiniLM-L6-v2 model for text embedding",
        "model_format": "TORCH_SCRIPT",
        "function_name": "TEXT_EMBEDDING",
        "model_group_id": "'${MODEL_GROUP_ID}'",
        "model_config": {
            "model_type": "bert",
            "embedding_dimension": 384,
            "framework_type": "sentence_transformers"
        }
    }')

echo "📋 Registration response: ${REGISTER_RESPONSE}"

# Extract task ID from response
TASK_ID=$(echo $REGISTER_RESPONSE | grep -o '"task_id":"[^"]*' | cut -d'"' -f4)

if [ -z "$TASK_ID" ]; then
    echo "❌ Error: Failed to extract task_id from response"
    echo "Response: ${REGISTER_RESPONSE}"
    exit 1
fi

echo "✅ Model registration started with Task ID: ${TASK_ID}"

# Wait for model registration to complete
echo "⏳ Waiting for model registration to complete..."
while true; do
    TASK_STATUS=$(curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/tasks/${TASK_ID}" | grep -o '"state":"[^"]*' | cut -d'"' -f4)
    echo "📊 Task status: ${TASK_STATUS}"
    
    if [ "$TASK_STATUS" = "COMPLETED" ]; then
        echo "✅ Model registration completed successfully"
        # Get the model ID from the completed task
        MODEL_ID=$(curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/tasks/${TASK_ID}" | grep -o '"model_id":"[^"]*' | cut -d'"' -f4)
        break
    elif [ "$TASK_STATUS" = "FAILED" ]; then
        echo "❌ Model registration failed"
        curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/tasks/${TASK_ID}"
        exit 1
    fi
    
    sleep 5
done

# Deploy the model
echo "🚀 Deploying the model..."
DEPLOY_RESPONSE=$(curl -s -X POST "http://${OPENSEARCH_HOST}/_plugins/_ml/models/${MODEL_ID}/_deploy")
echo "📋 Deploy response: ${DEPLOY_RESPONSE}"

DEPLOY_TASK_ID=$(echo $DEPLOY_RESPONSE | grep -o '"task_id":"[^"]*' | cut -d'"' -f4)

if [ -z "$DEPLOY_TASK_ID" ]; then
    echo "❌ Error: Failed to extract deploy task_id from response"
    echo "Response: ${DEPLOY_RESPONSE}"
    exit 1
fi

echo "✅ Model deployment started with Task ID: ${DEPLOY_TASK_ID}"

# Wait for deployment to complete
echo "⏳ Waiting for model deployment to complete..."
while true; do
    DEPLOY_STATUS=$(curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/tasks/${DEPLOY_TASK_ID}" | grep -o '"state":"[^"]*' | cut -d'"' -f4)
    echo "📊 Deploy status: ${DEPLOY_STATUS}"
    
    if [ "$DEPLOY_STATUS" = "COMPLETED" ]; then
        echo "✅ Model deployment completed successfully"
        break
    elif [ "$DEPLOY_STATUS" = "FAILED" ]; then
        echo "❌ Model deployment failed"
        curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/tasks/${DEPLOY_TASK_ID}"
        exit 1
    fi
    
    sleep 5
done

# Verify the model is ready
echo "🔍 Verifying model status..."
MODEL_STATUS=$(curl -s "http://${OPENSEARCH_HOST}/_plugins/_ml/models/${MODEL_ID}")
echo "📋 Model status: ${MODEL_STATUS}"

echo "🎉 Setup complete!"
echo "📝 Model ID: ${MODEL_ID}"
echo "📝 Model Group ID: ${MODEL_GROUP_ID}"

# Save model ID to .env file
echo "💾 Saving model ID to .env file..."
if [ -f .env ]; then
    # Remove existing MODEL_ID line if it exists
    sed -i '/^MODEL_ID=/d' .env
fi
echo "MODEL_ID=${MODEL_ID}" >> .env
echo "✅ Model ID saved to .env"

echo ""
echo "💡 You can now use this model for text embedding with:"
echo "curl -X POST \"http://${OPENSEARCH_HOST}/_plugins/_ml/models/${MODEL_ID}/_predict\" \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -d '{\"text_docs\": [\"your text here\"]}'"