#!/usr/bin/env python3

import requests
import time
import os
import sys
from typing import Optional

class OpenSearchModelSetup:
    def __init__(self, opensearch_url: str = "http://localhost:9200"):
        self.opensearch_url = opensearch_url
        self.headers = {'Content-Type': 'application/json'}
        self.model_group_name = "local_model_group"
        self.model_group_id = "local_models"
        self.model_name = "huggingface/sentence-transformers/all-MiniLM-L6-v2"
        self.model_version = "1.0.2"
        self.pipeline_name = "tweet_ingest"
        self.input_field = "full_text"
        self.output_field = "full_text_embedding"
        
    def check_opensearch_connection(self) -> bool:
        """Check if OpenSearch is accessible"""
        try:
            response = requests.get(f"{self.opensearch_url}/")
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to OpenSearch: {e}")
            return False
    
    def configure_ml_settings(self) -> bool:
        """Configure ML settings to allow running on data nodes and set memory threshold"""
        print("Configuring ML cluster settings...")
        
        payload = {
            "persistent": {
                "plugins.ml_commons.only_run_on_ml_node": "false",
                "plugins.ml_commons.native_memory_threshold": "99"
            }
        }
        
        try:
            response = requests.put(
                f"{self.opensearch_url}/_cluster/settings",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code in [200, 201]:
                print("ML cluster settings configured successfully")
                return True
            else:
                print(f"Failed to configure ML settings: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error configuring ML settings: {e}")
            return False
    
    def check_model_group_exists(self) -> Optional[str]:
        """Check if the model group already exists and return its ID"""
        try:
            # Search for model groups
            response = requests.get(f"{self.opensearch_url}/_plugins/_ml/model_groups/_search")
            if response.status_code == 200:
                data = response.json()
                for hit in data.get("hits", {}).get("hits", []):
                    source = hit.get("_source", {})
                    if source.get("name") == self.model_group_name:
                        group_id = hit.get("_id")
                        print(f"Model group '{self.model_group_name}' already exists with ID: {group_id}")
                        return group_id
            
            # Also try to search by query to be more thorough
            search_payload = {
                "query": {
                    "match": {
                        "name": self.model_group_name
                    }
                }
            }
            response = requests.post(
                f"{self.opensearch_url}/_plugins/_ml/model_groups/_search",
                headers=self.headers,
                json=search_payload
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("hits", {}).get("total", {}).get("value", 0) > 0:
                    hit = data.get("hits", {}).get("hits", [])[0]
                    group_id = hit.get("_id")
                    print(f"Model group '{self.model_group_name}' already exists with ID: {group_id}")
                    return group_id
            
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error checking model group: {e}")
            return None
            
    def create_model_group(self) -> Optional[str]:
        """Create the model group and return its ID"""
        print(f"Creating model group '{self.model_group_name}'...")
        
        payload = {
            "name": self.model_group_name,
            "description": "A model group for local models"
        }
        
        try:
            response = requests.post(
                f"{self.opensearch_url}/_plugins/_ml/model_groups/_register",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                group_id = result.get("model_group_id")
                print(f"Model group created successfully with ID: {group_id}")
                return group_id
            else:
                print(f"Failed to create model group: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error creating model group: {e}")
            return None
    
    def check_model_exists(self) -> Optional[str]:
        """Check if the model already exists and return model_id if found"""
        try:
            search_payload = {
                "query": {
                    "match": {
                        "name": self.model_name
                    }
                },
                "size": 1000
            }
            
            response = requests.post(
                f"{self.opensearch_url}/_plugins/_ml/models/_search",
                headers=self.headers,
                json=search_payload
            )
            
            if response.status_code == 200:
                data = response.json()
                for hit in data.get("hits", {}).get("hits", []):
                    source = hit.get("_source", {})
                    hit_id = hit.get("_id")
                    # Only consider the main model entry (not chunks with _N suffix)
                    if (source.get("name") == self.model_name and 
                        not hit_id.endswith(tuple(f"_{i}" for i in range(10)))):
                        model_id = hit_id
                        print(f"Model '{self.model_name}' already exists with ID: {model_id}")
                        return model_id
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error checking model: {e}")
            return None
    
    def register_model(self, model_group_id: str) -> Optional[str]:
        """Register the model and return task_id"""
        print(f"Registering model '{self.model_name}' in group {model_group_id}...")
        
        payload = {
            "name": self.model_name,
            "version": self.model_version,
            "model_group_id": model_group_id,
            "model_format": "TORCH_SCRIPT"
        }
        
        try:
            response = requests.post(
                f"{self.opensearch_url}/_plugins/_ml/models/_register",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                task_id = result.get("task_id")
                print(f"Model registration started. Task ID: {task_id}")
                return task_id
            else:
                print(f"Failed to register model: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error registering model: {e}")
            return None
    
    def wait_for_task_completion(self, task_id: str, task_description: str = "Task") -> Optional[str]:
        """Wait for a task to complete and return model_id if applicable"""
        print(f"{task_description} in progress (Task ID: {task_id})...")
        
        max_attempts = 60  # 5 minutes with 5-second intervals
        attempt = 0
        
        while attempt < max_attempts:
            try:
                response = requests.get(f"{self.opensearch_url}/_plugins/_ml/tasks/{task_id}")
                
                if response.status_code == 200:
                    result = response.json()
                    state = result.get("state", "")
                    
                    if state == "COMPLETED":
                        model_id = result.get("model_id")
                        print(f"{task_description} completed successfully!")
                        if model_id:
                            print(f"Model ID: {model_id}")
                        return model_id
                    elif state in ["FAILED", "CANCELLED"]:
                        error = result.get("error", "Unknown error")
                        print(f"{task_description} failed: {error}")
                        return None
                    else:
                        print(f"{task_description} status: {state}")
                        
                else:
                    print(f"Error checking task status: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Error checking task: {e}")
            
            attempt += 1
            time.sleep(5)
        
        print(f"{task_description} timed out after {max_attempts * 5} seconds")
        return None
    
    def check_model_deployment_status(self, model_id: str) -> str:
        """Check if the model is already deployed"""
        try:
            response = requests.get(f"{self.opensearch_url}/_plugins/_ml/models/{model_id}")
            
            if response.status_code == 200:
                result = response.json()
                model_state = result.get("model_state", "")
                print(f"Model current state: {model_state}")
                return model_state
            else:
                print(f"Error checking model status: {response.status_code}")
                return "UNKNOWN"
                
        except requests.exceptions.RequestException as e:
            print(f"Error checking model deployment status: {e}")
            return "UNKNOWN"

    def deploy_model(self, model_id: str) -> Optional[str]:
        """Deploy the model and return task_id"""
        print(f"Deploying model '{model_id}'...")
        
        try:
            response = requests.post(
                f"{self.opensearch_url}/_plugins/_ml/models/{model_id}/_deploy",
                headers=self.headers
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                task_id = result.get("task_id")
                print(f"Model deployment started. Task ID: {task_id}")
                return task_id
            else:
                print(f"Failed to deploy model: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error deploying model: {e}")
            return None
    
    def save_model_id_to_env(self, model_id: str) -> bool:
        """Save the model_id to .env file"""
        env_file = ".env"
        env_content = {}
        
        # Read existing .env file if it exists
        if os.path.exists(env_file):
            try:
                with open(env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and '=' in line and not line.startswith('#'):
                            key, value = line.split('=', 1)
                            env_content[key.strip()] = value.strip()
            except Exception as e:
                print(f"Warning: Could not read existing .env file: {e}")
        
        # Update or add MODEL_ID
        env_content['MODEL_ID'] = model_id
        
        # Write back to .env file
        try:
            with open(env_file, 'w') as f:
                for key, value in env_content.items():
                    f.write(f"{key}={value}\n")
            print(f"Model ID saved to {env_file}: {model_id}")
            return True
        except Exception as e:
            print(f"Error saving to .env file: {e}")
            return False
    
    def create_ingestion_pipeline(self, model_id: str) -> bool:
        """Create an ingestion pipeline for automatic text embedding"""
        print(f"Creating ingestion pipeline '{self.pipeline_name}' with model {model_id}...")
        
        payload = {
            "description": f"Ingest pipeline that automatically converts {self.input_field} to embeddings",
            "processors": [
                {
                    "text_embedding": {
                        "model_id": model_id,
                        "field_map": {
                            self.input_field: self.output_field
                        }
                    }
                }
            ]
        }
        
        try:
            response = requests.put(
                f"{self.opensearch_url}/_ingest/pipeline/{self.pipeline_name}",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code in [200, 201]:
                print(f"Ingestion pipeline '{self.pipeline_name}' created successfully")
                print(f"Pipeline will convert '{self.input_field}' field to '{self.output_field}' embeddings")
                return True
            else:
                print(f"Failed to create ingestion pipeline: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error creating ingestion pipeline: {e}")
            return False

def main():
    setup = OpenSearchModelSetup()
    
    # Check OpenSearch connection
    if not setup.check_opensearch_connection():
        print("Failed to connect to OpenSearch. Make sure it's running on http://localhost:9200")
        sys.exit(1)
    
    print("Connected to OpenSearch successfully")
    
    # Configure ML settings
    if not setup.configure_ml_settings():
        print("Failed to configure ML settings")
        sys.exit(1)
    
    # Check and create model group if needed
    model_group_id = setup.check_model_group_exists()
    if not model_group_id:
        model_group_id = setup.create_model_group()
        if not model_group_id:
            print("Failed to create model group")
            sys.exit(1)
    
    print("Model group setup completed")
    
    # Check if model already exists
    model_id = setup.check_model_exists()
    
    if not model_id:
        # Register the model
        task_id = setup.register_model(model_group_id)
        if not task_id:
            print("Failed to register model")
            sys.exit(1)
        
        # Wait for registration to complete
        model_id = setup.wait_for_task_completion(task_id, "Model registration")
        if not model_id:
            print("Model registration failed or timed out")
            sys.exit(1)
    
    # Save model ID to .env file
    if not setup.save_model_id_to_env(model_id):
        print("Warning: Failed to save model ID to .env file")
    
    # Check if model is already deployed
    model_state = setup.check_model_deployment_status(model_id)
    
    if model_state == "DEPLOYED":
        print(f"Model '{model_id}' is already deployed and ready to use")
    else:
        print(f"Model state is '{model_state}', proceeding with deployment...")
        
        # Deploy the model
        deploy_task_id = setup.deploy_model(model_id)
        if not deploy_task_id:
            print("Failed to start model deployment")
            sys.exit(1)
        
        # Wait for deployment to complete
        deployment_result = setup.wait_for_task_completion(deploy_task_id, "Model deployment")
        if deployment_result is None:
            print("Model deployment failed or timed out")
            sys.exit(1)
    
    # Create the ingestion pipeline
    if not setup.create_ingestion_pipeline(model_id):
        print("Warning: Failed to create ingestion pipeline, but model is still ready")
    
    print("Model setup completed successfully!")
    print(f"Model ID: {model_id}")
    print(f"Ingestion pipeline: {setup.pipeline_name}")
    print("The model and pipeline are now ready for use.")

if __name__ == "__main__":
    main()