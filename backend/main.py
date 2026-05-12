import json
import os
import boto3
import base64
import time
from decimal import Decimal
from typing import Dict, Any
from botocore.exceptions import ClientError
from botocore.config import Config

# Initialize AWS clients
REGION = os.environ.get('REGION', 'ap-northeast-2')
s3_client = boto3.client('s3', region_name=REGION,
                         config=Config(s3={'addressing_style': 'virtual'}))
bedrock_client = boto3.client('bedrock-runtime', region_name=REGION)
# DynamoDB configuration
dynamodb = boto3.resource('dynamodb', region_name=REGION)
metadata_table = None

def get_metadata_table():
    global metadata_table
    if metadata_table is None:
        table_name = os.environ.get('METADATA_TABLE')
        if table_name:
            metadata_table = dynamodb.Table(table_name)
    return metadata_table

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    raise TypeError

# OpenSearch configuration - initialize only when needed
opensearch_client = None

VECTOR_DIMENSION = 512  # Marengo 3.0 uses 512 dimensions

DAILY_LIMITS = {
    'analyzeCount': 20,
    'embedCount': 10,
    'searchCount': 100,
}

def check_and_increment_usage(user_id, usage_type, cors_headers):
    """Atomic check-and-increment daily usage. Returns error response if over limit, None if OK."""
    try:
        table = get_metadata_table()
        if not table:
            return None
        today = time.strftime('%Y-%m-%d', time.gmtime())
        limit = DAILY_LIMITS.get(usage_type, 100)
        ttl_value = int(time.time()) + 7 * 86400
        table.update_item(
            Key={'userId': user_id, 'sortKey': f'USAGE#{today}'},
            UpdateExpression='SET #c = if_not_exists(#c, :zero) + :one, #ttl = if_not_exists(#ttl, :ttl)',
            ConditionExpression='attribute_not_exists(#c) OR #c < :limit',
            ExpressionAttributeNames={'#c': usage_type, '#ttl': 'ttl'},
            ExpressionAttributeValues={':zero': 0, ':one': 1, ':limit': limit, ':ttl': ttl_value},
        )
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 429,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Daily limit reached ({limit} {usage_type.replace("Count","")}s/day). Try again tomorrow.'})
            }
        print(f"Usage check error: {e}")
        return None

SHARED_USER_ID = '__shared__'
ADMIN_USER_SUBS = {s.strip() for s in os.environ.get('ADMIN_USER_SUBS', '').split(',') if s.strip()}

def get_user_id(event):
    """Extract user ID from Cognito JWT claims via API Gateway authorizer"""
    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
    return claims.get('sub', 'anonymous')

def is_admin(event):
    return get_user_id(event) in ADMIN_USER_SUBS

def verify_video_s3_uri(s3_uri, user_id, allow_shared=True):
    """s3Uri가 본인 또는 공유 영상 prefix에 속하는지 검증. 위반 시 (False, reason) 반환."""
    if not s3_uri or not s3_uri.startswith('s3://'):
        return False, 'Invalid S3 URI format'
    parts = s3_uri[5:].split('/', 1)
    if len(parts) != 2:
        return False, 'Invalid S3 URI format'
    bucket, key = parts
    expected_bucket = os.environ.get('VIDEO_BUCKET')
    if expected_bucket and bucket != expected_bucket:
        return False, 'Bucket not allowed'
    allowed_prefixes = [f"videos/{user_id}/"]
    if allow_shared:
        allowed_prefixes.append(f"videos/{SHARED_USER_ID}/")
    if not any(key.startswith(p) for p in allowed_prefixes):
        return False, 'Access denied: you do not own this video'
    return True, None

def get_account_id():
    """Get AWS Account ID dynamically"""
    account_id = os.environ.get('AWS_ACCOUNT_ID')
    if not account_id:
        # Get account ID dynamically from AWS STS
        try:
            sts_client = boto3.client('sts', region_name=os.environ.get('REGION', 'ap-northeast-2'))
            account_id = sts_client.get_caller_identity()['Account']
            print(f"Dynamically retrieved AWS Account ID: {account_id}")
        except Exception as e:
            print(f"Error retrieving account ID: {e}")
            raise ValueError("AWS_ACCOUNT_ID environment variable not set and unable to retrieve from STS")
    return account_id

def get_opensearch_client():
    """Initialize OpenSearch client lazily"""
    global opensearch_client
    if opensearch_client is None:
        try:
            print("Initializing OpenSearch client...")
            from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
            
            opensearch_endpoint = os.environ.get('OPENSEARCH_ENDPOINT', '').replace('https://', '')
            region = os.environ.get('REGION', 'ap-northeast-2')
            print(f"OpenSearch endpoint: {opensearch_endpoint}, region: {region}")
            
            credentials = boto3.Session().get_credentials()
            # Use AWSV4SignerAuth with 'aoss' service for OpenSearch Serverless
            awsauth = AWSV4SignerAuth(credentials, region, 'aoss')
            print("AWSV4SignerAuth created successfully for aoss service")

            opensearch_client = OpenSearch(
                hosts=[{'host': opensearch_endpoint, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
            print("OpenSearch client initialized successfully")
        except Exception as e:
            print(f"OpenSearch client initialization failed: {e}")
            opensearch_client = None
    
    return opensearch_client

def search_opensearch(query_embedding, top_k=10, user_id=None):
    """Search OpenSearch for similar embeddings, filtered by userId"""
    try:
        import time
        start_time = time.time()

        opensearch = get_opensearch_client()
        if not opensearch:
            raise Exception("OpenSearch client not available")

        # First check if index exists and get its mapping
        try:
            ensure_vector_index(opensearch)
        except Exception as e:
            if 'index_not_found_exception' in str(e).lower():
                return {
                    'results': [],
                    'total': 0,
                    'search_time_ms': 0,
                    'message': 'No videos indexed yet - upload and process videos with embeddings first'
                }
            raise

        knn_query = {
            "knn": {
                "embedding": {
                    "vector": query_embedding,
                    "k": top_k
                }
            }
        }

        if user_id:
            allowed_user_ids = [user_id, SHARED_USER_ID] if user_id != SHARED_USER_ID else [SHARED_USER_ID]
            search_body = {
                "size": top_k,
                "query": {
                    "bool": {
                        "must": [knn_query],
                        "filter": [{"terms": {"userId": allowed_user_ids}}]
                    }
                },
                "_source": ["userId", "videoId", "videoS3Uri", "segmentId", "startSec", "endSec", "duration", "embeddingOption", "metadata"]
            }
        else:
            search_body = {
                "size": top_k,
                "query": knn_query,
                "_source": ["userId", "videoId", "videoS3Uri", "segmentId", "startSec", "endSec", "duration", "embeddingOption", "metadata"]
            }
        
        search_response = opensearch.search(
            index='video-embeddings',
            body=search_body
        )
        
        search_time = time.time() - start_time
        
        results = []
        for hit in search_response['hits']['hits']:
            source = hit['_source']
            results.append({
                'videoId': source.get('videoId', 'unknown'),
                'videoS3Uri': source.get('videoS3Uri', ''),
                'segmentId': source.get('segmentId', ''),
                'startSec': source.get('startSec', 0),
                'endSec': source.get('endSec', 0),
                'duration': source.get('duration', 0),
                'embeddingOption': source.get('embeddingOption', 'visual-text'),
                'score': hit['_score'],
                'metadata': source.get('metadata', {}),
                'isShared': source.get('userId') == SHARED_USER_ID,
            })
        
        print(f"OpenSearch: Found {len(results)} results in {search_time:.3f}s")
        
        return {
            'results': results,
            'total': search_response['hits']['total']['value'],
            'search_time_ms': round(search_time * 1000, 2)
        }
        
    except Exception as e:
        if 'index_not_found_exception' in str(e).lower():
            return {
                'results': [],
                'total': 0,
                'search_time_ms': 0,
                'message': 'No videos indexed yet - upload and process videos with embeddings first'
            }
        print(f"Error searching OpenSearch: {e}")
        raise

def ensure_vector_index(opensearch_client):
    """Ensure the vector index exists with proper mapping"""
    index_name = 'video-embeddings'
    
    try:
        # Check if index exists
        if opensearch_client.indices.exists(index=index_name):
            print(f"Index {index_name} already exists")
            # Check current mapping
            try:
                mapping = opensearch_client.indices.get_mapping(index=index_name)
                print(f"Current index mapping: {json.dumps(mapping, indent=2)}")
                
                # Check if embedding field is knn_vector
                properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
                embedding_field = properties.get('embedding', {})
                if embedding_field.get('type') != 'knn_vector':
                    print(f"WARNING: embedding field type is {embedding_field.get('type')}, not knn_vector")
                    print("Deleting and recreating index with correct mapping...")
                    opensearch_client.indices.delete(index=index_name)
                else:
                    print("Index has correct knn_vector mapping")
                    return
            except Exception as e:
                print(f"Error checking mapping: {e}")
                return
        
        # Create index with knn_vector mapping and temporal fields
        index_body = {
            "settings": {
                "index": {
                    "knn": True,
                    "knn.algo_param.ef_search": 512
                }
            },
            "mappings": {
                "properties": {
                    "userId": {
                        "type": "keyword"
                    },
                    "videoId": {
                        "type": "keyword"
                    },
                    "videoS3Uri": {
                        "type": "keyword"
                    },
                    "segmentId": {
                        "type": "keyword"
                    },
                    "startSec": {
                        "type": "float"
                    },
                    "endSec": {
                        "type": "float"
                    },
                    "duration": {
                        "type": "float"
                    },
                    "embeddingOption": {
                        "type": "keyword"
                    },
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": 512,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {
                                "ef_construction": 512,
                                "m": 16
                            }
                        }
                    },
                    "metadata": {
                        "type": "object"
                    }
                }
            }
        }
        
        opensearch_client.indices.create(index=index_name, body=index_body)
        print(f"Created index {index_name} with knn_vector mapping")
        
    except Exception as e:
        print(f"Error ensuring vector index: {e}")
        raise

def store_embeddings_to_opensearch(bedrock_response, embedding_data_list, original_s3_uri=None, user_id=None):
    """Store video embeddings with temporal segments to OpenSearch for similarity search"""
    print("🗂️ === OPENSEARCH EMBEDDING STORAGE START ===")
    
    opensearch = get_opensearch_client()
    if not opensearch:
        raise Exception("OpenSearch client not available")
    
    # ALWAYS ensure index exists with proper mapping BEFORE storing documents
    ensure_vector_index(opensearch)
    
    # Extract video metadata
    video_s3_uri = original_s3_uri or ''
    video_id = 'unknown'

    # If original_s3_uri not provided, try to extract from bedrock response
    if not video_s3_uri:
        model_input = bedrock_response.get('modelInput', {})
        media_source = model_input.get('mediaSource', {})
        s3_location = media_source.get('s3Location', {})
        video_s3_uri = s3_location.get('uri', '')

    # Extract from output path structure if needed
    if not video_s3_uri:
        output_data_config = bedrock_response.get('outputDataConfig', {})
        s3_output_config = output_data_config.get('s3OutputDataConfig', {})
        output_s3_uri = s3_output_config.get('s3Uri', '')
        if output_s3_uri and '/embeddings/' in output_s3_uri:
            path_parts = output_s3_uri.replace('s3://', '').split('/')
            bucket_name_parsed = path_parts[0]
            try:
                embeddings_index = path_parts.index('embeddings')
                # Skip user_id folder (embeddings/{user_id}/{video_id}/)
                # Find the last non-empty segment as video_id
                remaining = [p for p in path_parts[embeddings_index + 1:] if p]
                if len(remaining) >= 2:
                    # Path is embeddings/{user_id}/{video_id}/
                    user_id_from_path = remaining[0]
                    extracted_folder_name = remaining[1]
                elif len(remaining) == 1:
                    user_id_from_path = None
                    extracted_folder_name = remaining[0]
                else:
                    user_id_from_path = None
                    extracted_folder_name = None
                if extracted_folder_name:
                    video_filename = f"{extracted_folder_name}.mp4"
                    if user_id_from_path:
                        video_s3_uri = f"s3://{bucket_name_parsed}/videos/{user_id_from_path}/{video_filename}"
                    else:
                        video_s3_uri = f"s3://{bucket_name_parsed}/videos/{video_filename}"
                    video_id = extracted_folder_name
            except (ValueError, IndexError):
                pass

    # Fallback: extract from S3 URI
    if video_id == 'unknown' and video_s3_uri and video_s3_uri.startswith('s3://'):
        extracted_id = video_s3_uri.split('/')[-1]
        if '.' in extracted_id:
            video_id = extracted_id.rsplit('.', 1)[0]
        else:
            video_id = extracted_id
    
    import time
    start_time = time.time()
    
    print(f"🗂️ Processing OpenSearch storage for video: {video_id}, S3 URI: {video_s3_uri}")
    
    stored_count = 0
    responses = []
    
    # Handle both single embedding and list of embeddings
    if not isinstance(embedding_data_list, list):
        embedding_data_list = [embedding_data_list]
    
    # Skip if this (userId, videoId) pair is already indexed to avoid duplicates
    # on re-runs (OpenSearch Serverless does not support explicit doc IDs / upsert).
    try:
        probe = opensearch.search(index='video-embeddings', body={
            "size": 1,
            "query": {"bool": {"filter": [
                {"term": {"userId": user_id or 'unknown'}},
                {"term": {"videoId": video_id}},
            ]}},
            "_source": False,
        })
        if probe.get('hits', {}).get('total', {}).get('value', 0) > 0:
            print(f"OpenSearch: skip re-index for {user_id}/{video_id} (already has docs)")
            return {
                'stored_count': 0,
                'video_id': video_id,
                'skipped': True,
                'storage_time_ms': round((time.time() - start_time) * 1000, 2)
            }
    except Exception as e:
        print(f"Duplicate probe failed (will proceed): {e}")

    # Store each temporal segment as a separate document
    for i, embedding_data in enumerate(embedding_data_list):
        # Create unique document ID for each segment
        segment_id = f"{video_id}_segment_{i}_{embedding_data.get('startSec', 0)}"

        # Prepare document for OpenSearch
        document = {
            'userId': user_id or 'unknown',
            'videoId': video_id,
            'videoS3Uri': video_s3_uri,
            'segmentId': segment_id,
            'startSec': embedding_data.get('startSec', 0),
            'endSec': embedding_data.get('endSec', 0),
            'duration': embedding_data.get('endSec', 0) - embedding_data.get('startSec', 0),
            'embedding': embedding_data.get('embedding', []),
            'embeddingOption': embedding_data.get('embeddingOption', 'visual-text'),
            'metadata': {
                'modelId': bedrock_response.get('modelId', ''),
                'invocationArn': bedrock_response.get('invocationArn', ''),
                'timestamp': bedrock_response.get('endTime', ''),
                'segmentIndex': i,
                'totalSegments': len(embedding_data_list)
            }
        }
        
        print(f"Storing segment {i+1}/{len(embedding_data_list)}: {embedding_data.get('startSec', 0)}-{embedding_data.get('endSec', 0)}s, embedding length: {len(document['embedding'])}, type: {document['embeddingOption']}")
        
        # Index the document without explicit ID (OpenSearch Serverless doesn't support it)
        response = opensearch.index(
            index='video-embeddings',
            body=document
        )
        
        responses.append(response)
        stored_count += 1
    
    storage_time = time.time() - start_time
    print(f"OpenSearch: Stored {stored_count} segments in {storage_time:.3f}s")
    # Return simplified response to avoid Lambda 413 error with large responses
    return {
        'stored_count': stored_count, 
        'video_id': video_id,
        'storage_time_ms': round(storage_time * 1000, 2)
    }

def handle_flush_opensearch(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Flush/delete all documents from the OpenSearch vector index (admin only)"""
    try:
        if not is_admin(event):
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Admin access required'})
            }

        print("🗑️ Starting OpenSearch index flush...")
        
        opensearch = get_opensearch_client()
        if not opensearch:
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'OpenSearch client not available'})
            }
        
        index_name = 'video-embeddings'
        
        # Check if index exists
        if not opensearch.indices.exists(index=index_name):
            print(f"Index {index_name} does not exist")
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': f'Index {index_name} does not exist - nothing to flush',
                    'documents_deleted': 0
                })
            }
        
        # Get current document count
        try:
            count_response = opensearch.count(index=index_name)
            total_docs = count_response.get('count', 0)
            print(f"Found {total_docs} documents to delete")
        except Exception as e:
            print(f"Could not get document count: {e}")
            total_docs = "unknown"

        # Delete entire index and recreate (delete_by_query not supported on AOSS)
        opensearch.indices.delete(index=index_name)
        print(f"Deleted index {index_name}")
        ensure_vector_index(opensearch)
        print(f"Recreated index {index_name}")

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Successfully flushed OpenSearch index {index_name}',
                'documents_before': total_docs,
                'documents_deleted': total_docs
            })
        }
        
    except Exception as e:
        print(f"Error flushing OpenSearch: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Failed to flush OpenSearch: {str(e)}'})
        }

def process_analysis_async(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process video analysis asynchronously (called via direct Lambda invoke)"""
    try:
        print("=== ASYNC ANALYSIS PROCESSING START ===")
        
        analysis_job_id = event.get('analysisJobId')
        s3_uri = event.get('s3Uri')
        prompt = event.get('prompt')
        video_id = event.get('videoId')
        bucket_name = event.get('bucketName')
        user_id = event.get('userId', 'anonymous')

        print(f"Processing async analysis - Job ID: {analysis_job_id}")
        print(f"S3 URI: {s3_uri}, Video ID: {video_id}")
        print(f"Prompt length: {len(prompt) if prompt else 0}")
        
        if not all([analysis_job_id, s3_uri, prompt, bucket_name]):
            raise ValueError("Missing required parameters for async analysis processing")
        
        import time
        start_time = time.time()
        
        # Use invoke_model for Pegasus
        request_body = {
            "inputPrompt": prompt,
            "mediaSource": {
                "s3Location": {
                    "uri": s3_uri,
                    "bucketOwner": get_account_id()
                }
            },
            "temperature": 0.2,
            "maxOutputTokens": 4096
        }
        
        print(f"Calling Bedrock Pegasus model with request: {json.dumps(request_body, indent=2)}")
        
        response = bedrock_client.invoke_model(
            modelId='apac.twelvelabs.pegasus-1-2-v1:0',
            body=json.dumps(request_body),
            contentType='application/json'
        )
        
        print(f"Bedrock response status: {response['ResponseMetadata']['HTTPStatusCode']}")
        response_body = json.loads(response['body'].read())
        print(f"Analysis completed successfully. Response keys: {list(response_body.keys())}")
        
        # Store the analysis result in S3
        analysis_result = {
            'jobId': analysis_job_id,
            'status': 'Completed',
            'videoId': video_id,
            's3Uri': s3_uri,
            'prompt': prompt,
            'analysis': response_body.get('message', ''),
            'finishReason': response_body.get('finishReason', ''),
            'endTime': time.time(),
            'completedTime': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            'processingTimeSeconds': time.time() - start_time
        }
        
        # Store completed result
        result_key = f"analysis/{user_id}/{analysis_job_id}/result.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=result_key,
            Body=json.dumps(analysis_result, indent=2),
            ContentType='application/json'
        )
        
        # Update job status
        job_key = f"analysis/{user_id}/{analysis_job_id}/job_info.json"
        job_info = {
            'jobId': analysis_job_id,
            'status': 'Completed',
            'videoId': video_id,
            's3Uri': s3_uri,
            'prompt': prompt,
            'endTime': time.time(),
            'completedTime': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            'processingTimeSeconds': time.time() - start_time
        }
        s3_client.put_object(
            Bucket=bucket_name,
            Key=job_key,
            Body=json.dumps(job_info, indent=2),
            ContentType='application/json'
        )
        
        # Update analysis record in DDB
        try:
            analysis_sort_key = event.get('analysisSortKey')
            if analysis_sort_key:
                table = get_metadata_table()
                if table:
                    table.update_item(
                        Key={'userId': user_id, 'sortKey': analysis_sort_key},
                        UpdateExpression='SET #s = :s, analysis = :a, completedAt = :t, completedAtISO = :iso',
                        ExpressionAttributeNames={'#s': 'status'},
                        ExpressionAttributeValues={
                            ':s': 'Completed',
                            ':a': response_body.get('message', ''),
                            ':t': int(time.time()),
                            ':iso': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                        }
                    )
        except Exception as e:
            print(f"Failed to update analysis record in DDB: {e}")

        print(f"Analysis completed and stored at s3://{bucket_name}/{result_key}")
        print(f"Processing time: {time.time() - start_time:.2f} seconds")
        print("=== ASYNC ANALYSIS PROCESSING END ===")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'jobId': analysis_job_id,
                'status': 'Completed',
                'processingTime': time.time() - start_time
            })
        }
        
    except Exception as e:
        print(f"Async analysis processing failed: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        
        # Update job status to failed if we have the required info
        if 'analysis_job_id' in locals() and 'bucket_name' in locals():
            try:
                job_key = f"analysis/{user_id}/{analysis_job_id}/job_info.json"
                failed_job_info = {
                    'jobId': analysis_job_id,
                    'status': 'Failed',
                    'error': str(e),
                    'endTime': time.time(),
                    'failedTime': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
                }
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=job_key,
                    Body=json.dumps(failed_job_info, indent=2),
                    ContentType='application/json'
                )
                print(f"Updated job status to failed in S3")
            except Exception as update_error:
                print(f"Failed to update job status: {update_error}")

        try:
            analysis_sort_key = event.get('analysisSortKey')
            if analysis_sort_key and 'user_id' in locals():
                table = get_metadata_table()
                if table:
                    table.update_item(
                        Key={'userId': user_id, 'sortKey': analysis_sort_key},
                        UpdateExpression='SET #s = :s, #err = :e',
                        ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
                        ExpressionAttributeValues={':s': 'Failed', ':e': str(e)}
                    )
        except Exception as ddb_error:
            print(f"Failed to update analysis status in DDB: {ddb_error}")

        print("=== ASYNC ANALYSIS PROCESSING END (ERROR) ===")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'jobId': locals().get('analysis_job_id', 'unknown')
            })
        }

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler for video understanding API"""

    if 'action' in event and event.get('action') == 'process_analysis':
        print("Processing async analysis request")
        return process_analysis_async(event)

    if event.get('internalAction') == 'embed_shared_sample':
        process_shared_sample_embedding(event['s3Uri'], event['videoId'], event.get('queue', []))
        return {'status': 'ok'}
    
    print(f"Received event: {event.get('httpMethod')} {event.get('path')}")
    event_body = event.get('body', 'No body')
    if event_body and event_body != 'No body':
        print(f"Event body preview: {event_body[:200]}...")
    else:
        print("Event body: None or empty")
    print(f"Context: {context.function_name} - {context.aws_request_id}")
    
    # CORS headers
    cors_headers = {
        'Access-Control-Allow-Origin': os.environ.get('CORS_ORIGIN', 'http://localhost:3000'),
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-Amz-Date, Authorization, X-Api-Key, X-Amz-Security-Token',
        'Content-Type': 'application/json'
    }
    
    try:
        path = event.get('path', '')
        method = event.get('httpMethod', '')
        
        # Handle preflight OPTIONS requests
        if method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': ''
            }
        
        print(f"Processing request: {method} {path}")
        
        if path == '/upload' and method == 'POST':
            print("Routing to handle_upload")
            return handle_upload(event, cors_headers)
        elif path == '/upload-confirm' and method == 'POST':
            print("Routing to handle_upload_confirm")
            return handle_upload_confirm(event, cors_headers)
        elif path == '/analyze' and method == 'POST':
            print("Routing to handle_analyze")
            return handle_analyze(event, cors_headers, context)
        elif path == '/embed' and method == 'POST':
            print("Routing to handle_embed")
            return handle_embed(event, cors_headers)
        elif path == '/status' and method == 'GET':
            print("Routing to handle_status")
            return handle_status(event, cors_headers)
        elif path == '/search' and method == 'GET':
            print("Routing to handle_search")
            return handle_search(event, cors_headers)
        elif path == '/video-url' and method == 'GET':
            print("Routing to handle_video_url")
            return handle_video_url(event, cors_headers)
        elif path == '/flush-opensearch' and method == 'POST':
            print("Routing to handle_flush_opensearch")
            return handle_flush_opensearch(event, cors_headers)
        elif path == '/videos' and method == 'GET':
            print("Routing to handle_list_videos")
            return handle_list_videos(event, cors_headers)
        elif path == '/analyses' and method == 'GET':
            print("Routing to handle_list_analyses")
            return handle_list_analyses(event, cors_headers)
        elif path == '/embeddings' and method == 'GET':
            print("Routing to handle_list_embeddings")
            return handle_list_embeddings(event, cors_headers)
        elif path == '/admin/index-samples' and method == 'POST':
            print("Routing to handle_index_samples")
            return handle_index_samples(event, cors_headers)
        else:
            print(f"No route found for {method} {path}")
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Not found'})
            }
    
    except Exception as e:
        print(f"CRITICAL ERROR in main handler: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }

def handle_video_url(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Generate presigned URL for video playback"""
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        video_s3_uri = query_params.get('videoS3Uri')

        print(f"📹 Video URL request: {video_s3_uri}")
        
        if not video_s3_uri:
            print("❌ ERROR: videoS3Uri parameter is required but not provided")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'videoS3Uri parameter is required'})
            }
        
        # Parse S3 URI to get bucket and key
        if not video_s3_uri.startswith('s3://'):
            print(f"❌ ERROR: Invalid S3 URI format: {video_s3_uri}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid S3 URI format'})
            }
        
        # Remove s3:// prefix and split bucket/key
        s3_path = video_s3_uri[5:]  # Remove 's3://'
        parts = s3_path.split('/', 1)
        print(f"🔗 S3 path after removing s3://: {s3_path}")
        print(f"🪣 Parsed parts: {parts}")
        
        if len(parts) != 2:
            print(f"❌ ERROR: Invalid S3 URI format - could not split bucket/key: {parts}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid S3 URI format'})
            }
        
        bucket_name, object_key = parts
        print(f"🪣 Bucket: {bucket_name}")
        print(f"🔑 Object key: {object_key}")

        # Verify ownership: object must belong to caller or shared prefix
        user_id = get_user_id(event)
        allowed_prefixes = [f"videos/{user_id}/", f"videos/{SHARED_USER_ID}/"]
        if not any(object_key.startswith(prefix) for prefix in allowed_prefixes):
            print(f"❌ Access denied: {object_key} not owned by {user_id}")
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Access denied: you do not own this video'})
            }

        # Check if object exists before generating presigned URL
        try:
            print(f"🔍 Checking if object exists in S3...")
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            print(f"✅ Object exists in S3: {bucket_name}/{object_key}")
        except Exception as head_error:
            print(f"❌ Object does not exist in S3: {head_error}")
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Video file not found in S3: {object_key}'})
            }
        
        # Generate presigned URL for video access (valid for 1 hour)
        print(f"🔗 Generating presigned URL for {bucket_name}/{object_key}")
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=3600
        )
        
        print(f"✅ Generated presigned URL successfully for {bucket_name}/{object_key}")
        print(f"🌐 Presigned URL length: {len(presigned_url)}")
        print(f"🌐 Presigned URL preview: {presigned_url[:100]}...")
        response_data = {
            'presignedUrl': presigned_url,
            'videoS3Uri': video_s3_uri,
            'bucket': bucket_name,
            'key': object_key
        }
        
        print(f"✅ Returning successful response with data: {json.dumps(response_data, indent=2)}")
        print(f"🎬 === VIDEO URL REQUEST END ===")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(response_data)
        }
    
    except Exception as e:
        print(f"❌ ERROR in handle_video_url: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        import traceback
        print(f"❌ Full traceback: {traceback.format_exc()}")
        print(f"🎬 === VIDEO URL REQUEST END (ERROR) ===")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': str(e)})
        }

def handle_upload(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Handle video upload to S3"""
    try:
        body = json.loads(event.get('body', '{}'))
        filename = body.get('filename')
        content_type = body.get('contentType', 'video/mp4')
        
        if not filename:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Filename is required'})
            }

        # filename 경로 순회/서브디렉토리 차단
        if '/' in filename or '\\' in filename or filename.startswith('.') or '..' in filename:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid filename'})
            }

        bucket_name = os.environ.get('VIDEO_BUCKET')
        user_id = get_user_id(event)
        key = f"videos/{user_id}/{filename}"

        try:
            s3_client.head_object(Bucket=bucket_name, Key=key)
            return {
                'statusCode': 409,
                'headers': cors_headers,
                'body': json.dumps({'error': f'File "{filename}" already exists. Please rename and try again.'})
            }
        except ClientError:
            pass
        
        # Generate presigned POST instead of PUT
        presigned_post = s3_client.generate_presigned_post(
            Bucket=bucket_name,
            Key=key,
            Fields={'Content-Type': content_type},
            Conditions=[
                {'Content-Type': content_type},
                ['content-length-range', 1, 2147483648]  # 1 byte to 2GB
            ],
            ExpiresIn=3600
        )

        # Save video metadata to DDB as pending (TTL = 24h, promoted on confirm)
        try:
            table = get_metadata_table()
            if table:
                table.put_item(Item={
                    'userId': user_id,
                    'sortKey': f'VIDEO#{key}',
                    'filename': filename,
                    's3Uri': f's3://{bucket_name}/{key}',
                    'bucket': bucket_name,
                    'key': key,
                    'contentType': content_type,
                    'status': 'pending',
                    'uploadedAt': int(time.time()),
                    'uploadedAtISO': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                    'ttl': int(time.time()) + 86400,
                })
        except Exception as e:
            print(f"Failed to save video metadata to DDB: {e}")

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'uploadUrl': presigned_post['url'],
                'fields': presigned_post['fields'],
                'key': key,
                'bucket': bucket_name
            })
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': str(e)})
        }

def handle_upload_confirm(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Confirm upload succeeded — promote metadata from pending to active"""
    try:
        body = json.loads(event.get('body', '{}'))
        key = body.get('key')
        if not key:
            return {'statusCode': 400, 'headers': cors_headers, 'body': json.dumps({'error': 'key is required'})}

        user_id = get_user_id(event)
        bucket_name = os.environ.get('VIDEO_BUCKET')

        # key 소유권 검증 — 반드시 본인 prefix에 속해야 함
        if not key.startswith(f"videos/{user_id}/"):
            return {'statusCode': 403, 'headers': cors_headers, 'body': json.dumps({'error': 'Access denied'})}

        # Verify the object actually exists in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=key)
        except ClientError:
            return {'statusCode': 404, 'headers': cors_headers, 'body': json.dumps({'error': 'Upload not found in S3'})}

        # Promote: remove TTL and set status to active
        table = get_metadata_table()
        if table:
            table.update_item(
                Key={'userId': user_id, 'sortKey': f'VIDEO#{key}'},
                UpdateExpression='SET #s = :s REMOVE #ttl',
                ExpressionAttributeNames={'#s': 'status', '#ttl': 'ttl'},
                ExpressionAttributeValues={':s': 'active'},
            )

        return {'statusCode': 200, 'headers': cors_headers, 'body': json.dumps({'message': 'Upload confirmed', 'key': key})}
    except Exception as e:
        return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': str(e)})}

def wait_for_s3_object(s3_uri: str, max_wait_seconds: int = 30) -> bool:
    """Wait for S3 object to be available with exponential backoff"""
    if not s3_uri.startswith('s3://'):
        print(f"Invalid S3 URI format: {s3_uri}")
        return False
    
    # Parse S3 URI
    s3_path = s3_uri[5:]  # Remove 's3://'
    parts = s3_path.split('/', 1)
    if len(parts) != 2:
        print(f"Invalid S3 URI format: {s3_uri}")
        return False
    
    bucket_name, object_key = parts
    print(f"Checking S3 object existence: bucket={bucket_name}, key={object_key}")
    
    import time
    wait_time = 1  # Start with 1 second
    total_waited = 0
    
    while total_waited < max_wait_seconds:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = response.get('ContentLength', 0)
            print(f"S3 object found! Size: {file_size} bytes, waited {total_waited}s")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey':
                print(f"S3 object not found yet, waited {total_waited}s, retrying in {wait_time}s...")
                time.sleep(wait_time)
                total_waited += wait_time
                wait_time = min(wait_time * 1.5, 5)  # Exponential backoff, max 5s
            else:
                print(f"S3 error checking object: {error_code} - {e}")
                return False
        except Exception as e:
            print(f"Unexpected error checking S3 object: {e}")
            return False
    
    print(f"S3 object not found after waiting {max_wait_seconds} seconds")
    return False

def handle_analysis_status(analysis_job_id: str, cors_headers: Dict[str, str], event: Dict[str, Any]) -> Dict[str, Any]:
    """Check status of Pegasus analysis job and retrieve results from S3"""
    try:
        print(f"Checking analysis status for job: {analysis_job_id}")

        bucket_name = os.environ.get('VIDEO_BUCKET')
        user_id = get_user_id(event)
        job_key = f"analysis/{user_id}/{analysis_job_id}/job_info.json"
        result_key = f"analysis/{user_id}/{analysis_job_id}/result.json"
        
        # First, check if job info exists
        try:
            job_response = s3_client.get_object(Bucket=bucket_name, Key=job_key)
            job_info = json.loads(job_response['Body'].read())
            print(f"Found job info: {job_info.get('status', 'Unknown')}")
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                print(f"Analysis job {analysis_job_id} not found")
                return {
                    'statusCode': 404,
                    'headers': cors_headers,
                    'body': json.dumps({'error': f'Analysis job {analysis_job_id} not found'})
                }
            raise
        
        job_status = job_info.get('status', 'Unknown')
        
        if job_status == 'Completed':
            # Try to get the analysis result
            try:
                result_response = s3_client.get_object(Bucket=bucket_name, Key=result_key)
                result_data = json.loads(result_response['Body'].read())
                print(f"Retrieved analysis result for job {analysis_job_id}")
                
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'status': 'Completed',
                        'jobId': analysis_job_id,
                        'videoId': result_data.get('videoId', 'unknown'),
                        'analysis': result_data.get('analysis', ''),
                        'finishReason': result_data.get('finishReason', ''),
                        'prompt': result_data.get('prompt', ''),
                        'processingTime': result_data.get('processingTimeSeconds', 0),
                        'completedTime': result_data.get('completedTime', ''),
                        'message': 'Analysis completed successfully'
                    })
                }
                
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                    print(f"Result file not found for completed job {analysis_job_id}")
                    return {
                        'statusCode': 200,
                        'headers': cors_headers,
                        'body': json.dumps({
                            'status': 'Completed',
                            'message': 'Analysis completed but result file not found',
                            'jobId': analysis_job_id
                        })
                    }
                raise
                
        elif job_status == 'Failed':
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'status': 'Failed',
                    'jobId': analysis_job_id,
                    'error': job_info.get('error', 'Analysis failed'),
                    'message': 'Analysis failed'
                })
            }
        
        else:  # InProgress or other status
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'status': job_status,
                    'jobId': analysis_job_id,
                    'message': f'Analysis is {job_status.lower()}',
                    'videoId': job_info.get('videoId', 'unknown'),
                    'submitTime': job_info.get('submitTime', '')
                })
            }
            
    except Exception as e:
        print(f"Error checking analysis status: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Failed to check analysis status: {str(e)}'})
        }

def handle_analyze(event: Dict[str, Any], cors_headers: Dict[str, str], context: Any) -> Dict[str, Any]:
    """Handle video analysis using Twelve Labs Pegasus - start analysis and return job ID"""
    limit_err = check_and_increment_usage(get_user_id(event), 'analyzeCount', cors_headers)
    if limit_err:
        return limit_err
    try:
        print("Starting video analysis...")
        body = json.loads(event.get('body', '{}'))
        s3_uri = body.get('s3Uri')
        prompt = body.get('prompt', 'Analyze this video and provide a detailed description')
        video_id = body.get('videoId', 'unknown')
        
        print(f"Analysis request - S3 URI: {s3_uri}, Video ID: {video_id}, Prompt length: {len(prompt)}")
        
        if not s3_uri:
            print("ERROR: S3 URI is required but not provided")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'S3 URI is required'})
            }

        ok, reason = verify_video_s3_uri(s3_uri, get_user_id(event))
        if not ok:
            return {'statusCode': 403, 'headers': cors_headers, 'body': json.dumps({'error': reason})}

        # Wait for S3 object to be available
        if not wait_for_s3_object(s3_uri, max_wait_seconds=30):
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Video file not found in S3. Please ensure the upload completed successfully.'})
            }
        
        # Generate unique analysis job ID
        import uuid
        import time
        analysis_job_id = f"analysis_{int(time.time())}_{str(uuid.uuid4())[:8]}"
        
        # Create analysis job info to store in S3
        job_info = {
            'jobId': analysis_job_id,
            'status': 'InProgress',
            'videoId': video_id,
            's3Uri': s3_uri,
            'prompt': prompt,
            'startTime': time.time(),
            'submitTime': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
        }
        
        # Store job info in S3 first
        bucket_name = os.environ.get('VIDEO_BUCKET')
        user_id = get_user_id(event)
        job_key = f"analysis/{user_id}/{analysis_job_id}/job_info.json"
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=job_key,
                Body=json.dumps(job_info, indent=2),
                ContentType='application/json'
            )
            print(f"Stored analysis job info at s3://{bucket_name}/{job_key}")
        except Exception as e:
            print(f"Failed to store job info: {e}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Failed to initialize analysis job: {str(e)}'})
            }

        # Save analysis record to DDB
        analysis_sort_key = f'ANALYSIS#{video_id}#{int(time.time())}'
        try:
            table = get_metadata_table()
            if table:
                table.put_item(Item={
                    'userId': user_id,
                    'sortKey': analysis_sort_key,
                    'videoId': video_id,
                    'jobId': analysis_job_id,
                    'prompt': prompt,
                    's3Uri': s3_uri,
                    'status': 'InProgress',
                    'createdAt': int(time.time()),
                    'createdAtISO': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                })
        except Exception as e:
            print(f"Failed to save analysis record to DDB: {e}")

        # Invoke Lambda asynchronously to process the analysis
        try:
            lambda_client = boto3.client('lambda', region_name=os.environ.get('REGION', 'ap-northeast-2'))
            function_name = os.environ.get('LAMBDA_FUNCTION_NAME') or context.function_name
            
            # Create payload for async processing
            async_payload = {
                'action': 'process_analysis',  # Special action for async processing
                'analysisJobId': analysis_job_id,
                's3Uri': s3_uri,
                'prompt': prompt,
                'videoId': video_id,
                'bucketName': bucket_name,
                'userId': user_id,
                'analysisSortKey': analysis_sort_key
            }
            
            print(f"Invoking Lambda function asynchronously for job {analysis_job_id}")
            print(f"Function name: {function_name}")
            print(f"Async payload: {json.dumps(async_payload, indent=2)}")
            
            # Invoke Lambda asynchronously (Event invocation type)
            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(async_payload)
            )
            
            print(f"Lambda function invoked asynchronously for analysis job {analysis_job_id}")
            
        except Exception as e:
            print(f"Failed to invoke Lambda asynchronously: {e}")
            # Update job status to failed
            job_info.update({
                'status': 'Failed',
                'error': f'Failed to start async processing: {str(e)}',
                'endTime': time.time(),
                'failedTime': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
            })
            s3_client.put_object(
                Bucket=bucket_name,
                Key=job_key,
                Body=json.dumps(job_info, indent=2),
                ContentType='application/json'
            )
            
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Failed to start analysis: {str(e)}'})
            }
        
        # Return job ID immediately for status checking
        return {
            'statusCode': 202,
            'headers': cors_headers,
            'body': json.dumps({
                'analysisJobId': analysis_job_id,
                'status': 'processing',
                'message': 'Analysis started successfully. Use /status endpoint to check progress.',
                'videoId': video_id
            })
        }
    
    except json.JSONDecodeError as e:
        print(f"JSON decode error in analyze: {e}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Invalid JSON in request body: {str(e)}'})
        }
    except ClientError as e:
        print(f"AWS ClientError in analyze: {e}")
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        print(f"Error code: {error_code}, Message: {error_message}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'AWS Error ({error_code}): {error_message}'})
        }
    except Exception as e:
        print(f"Unexpected error in analyze: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Analysis failed: {str(e)}'})
        }

def handle_embed(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Handle video embedding generation using Twelve Labs Marengo (async)"""
    limit_err = check_and_increment_usage(get_user_id(event), 'embedCount', cors_headers)
    if limit_err:
        return limit_err
    try:
        print("Starting embedding generation...")
        body = json.loads(event.get('body', '{}'))
        s3_uri = body.get('s3Uri')
        video_id = body.get('videoId')
        
        print(f"Embedding request - S3 URI: {s3_uri}, Video ID: {video_id}")
        
        if not s3_uri or not video_id:
            print(f"ERROR: Missing required parameters - S3 URI: {bool(s3_uri)}, Video ID: {bool(video_id)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'S3 URI and video ID are required'})
            }

        ok, reason = verify_video_s3_uri(s3_uri, get_user_id(event))
        if not ok:
            return {'statusCode': 403, 'headers': cors_headers, 'body': json.dumps({'error': reason})}

        # Wait for S3 object to be available
        if not wait_for_s3_object(s3_uri, max_wait_seconds=45):
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Video file not found in S3. Please ensure the upload completed successfully.'})
            }
        
        # Use async invoke for Marengo 3.0 with temporal segmentation
        model_input = {
            "inputType": "video",
            "video": {
                "mediaSource": {
                    "s3Location": {
                        "uri": s3_uri,
                        "bucketOwner": get_account_id()
                    }
                },
                "segmentation": {
                    "method": "fixed",
                    "fixed": {
                        "durationSec": 10
                    }
                },
                "embeddingOption": ["visual", "audio"],
                "embeddingScope": ["clip"]
            }
        }
        
        print(f"Calling Bedrock Marengo model with input: {json.dumps(model_input, indent=2)}")
        
        # Create a unique embedding folder that includes the video_id for later retrieval
        # Clean the video_id to remove path prefixes but keep the filename with extension
        clean_video_id = video_id
        if '/' in clean_video_id:
            clean_video_id = clean_video_id.split('/')[-1]  # Remove path prefix like "videos/"
        
        # For the safe folder name, remove extension to avoid confusion
        safe_video_id = clean_video_id
        if '.' in safe_video_id:
            safe_video_id = safe_video_id.rsplit('.', 1)[0]  # Remove extension for folder name
        safe_video_id = safe_video_id.replace('/', '_').replace(' ', '_')  # Make filesystem safe
        
        user_id = get_user_id(event)
        print(f"🔍 DEBUG: Original video_id: '{video_id}', clean_video_id: '{clean_video_id}', safe_video_id: '{safe_video_id}'")
        response = bedrock_client.start_async_invoke(
            modelId='twelvelabs.marengo-embed-3-0-v1:0',
            modelInput=model_input,
            outputDataConfig={
                's3OutputDataConfig': {
                    's3Uri': f"s3://{os.environ.get('VIDEO_BUCKET')}/embeddings/{user_id}/{safe_video_id}/"
                }
            }
        )
        
        print(f"Bedrock async invoke response: {json.dumps(response, indent=2, default=str)}")
        
        invocation_arn = response.get('invocationArn')
        print(f"Successfully started embedding generation with ARN: {invocation_arn}")

        # Save embedding record to DDB
        try:
            table = get_metadata_table()
            if table:
                table.put_item(Item={
                    'userId': user_id,
                    'sortKey': f'EMBEDDING#{video_id}',
                    'videoId': video_id,
                    's3Uri': s3_uri,
                    'invocationArn': invocation_arn,
                    'status': 'processing',
                    'createdAt': int(time.time()),
                    'createdAtISO': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                })
        except Exception as e:
            print(f"Failed to save embedding record to DDB: {e}")

        return {
            'statusCode': 202,
            'headers': cors_headers,
            'body': json.dumps({
                'invocationArn': invocation_arn,
                'status': 'processing',
                'message': 'Embedding generation started'
            })
        }

    except json.JSONDecodeError as e:
        print(f"JSON decode error in embed: {e}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Invalid JSON in request body: {str(e)}'})
        }
    except ClientError as e:
        print(f"AWS ClientError in embed: {e}")
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        print(f"Error code: {error_code}, Message: {error_message}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'AWS Error ({error_code}): {error_message}'})
        }
    except Exception as e:
        print(f"Unexpected error in embed: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Embedding generation failed: {str(e)}'})
        }

def handle_status(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Check status of async invocation OR analysis job and retrieve results"""
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        invocation_arn = query_params.get('invocationArn')
        analysis_job_id = query_params.get('analysisJobId')
        
        print(f"Status check request - ARN: {invocation_arn}, Analysis Job ID: {analysis_job_id}")
        
        # Handle analysis job status check
        if analysis_job_id:
            return handle_analysis_status(analysis_job_id, cors_headers, event)
        
        # Handle embedding status check (existing functionality)
        if not invocation_arn:
            print("ERROR: Neither invocation ARN nor analysis job ID provided")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Either invocationArn or analysisJobId parameter is required'})
            }
        
        # Get invocation status
        print("Calling bedrock_client.get_async_invoke...")
        response = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
        
        status = response.get('status')
        print(f"Bedrock response status: {status}")
        
        if status == 'Completed':
            # Get the output S3 URI from Bedrock response
            output_data_config = response.get('outputDataConfig', {})
            s3_output_config = output_data_config.get('s3OutputDataConfig', {})
            output_s3_uri = s3_output_config.get('s3Uri')

            if output_s3_uri:
                # Bedrock creates: s3://bucket/embeddings/{invocationId}
                # The actual results are in: s3://bucket/embeddings/{invocationId}/output.json
                uri_parts = output_s3_uri.replace('s3://', '').split('/')
                bucket = uri_parts[0]
                key = '/'.join(uri_parts[1:]) + '/output.json'

                try:
                    print(f"Fetching result from S3: {bucket}/{key}")
                    s3_response = s3_client.get_object(Bucket=bucket, Key=key)
                    result_data = json.loads(s3_response['Body'].read())
                    print(f"Retrieved result data structure: {list(result_data.keys())}")

                    # Look up original s3Uri from DDB
                    original_s3_uri = None
                    embed_user = get_user_id(event)
                    try:
                        tbl = get_metadata_table()
                        if tbl:
                            ddb_resp = tbl.query(
                                KeyConditionExpression='userId = :uid AND begins_with(sortKey, :prefix)',
                                ExpressionAttributeValues={':uid': embed_user, ':prefix': 'EMBEDDING#'},
                            )
                            for itm in ddb_resp.get('Items', []):
                                if itm.get('invocationArn') == invocation_arn:
                                    original_s3_uri = itm.get('s3Uri', '')
                                    print(f"Found original s3Uri from DDB: {original_s3_uri}")
                                    break
                    except Exception as ddb_e:
                        print(f"Failed to lookup original s3Uri from DDB: {ddb_e}")

                    # Claim completion atomically to prevent duplicate indexing
                    storage_result = None
                    already_indexed = False
                    try:
                        tbl = get_metadata_table()
                        if tbl:
                            tbl.update_item(
                                Key={'userId': embed_user, 'sortKey': f'EMBEDDING#{invocation_arn}'},
                                UpdateExpression='SET #s = :s',
                                ConditionExpression='attribute_not_exists(#s) OR #s <> :s',
                                ExpressionAttributeNames={'#s': 'indexedStatus'},
                                ExpressionAttributeValues={':s': 'indexed'},
                            )
                    except ClientError as ce:
                        if ce.response['Error']['Code'] == 'ConditionalCheckFailedException':
                            already_indexed = True
                            print("Embedding already indexed by another request, skipping")
                        else:
                            print(f"Claim check error (will proceed): {ce}")

                    # Store embeddings to OpenSearch only if not already indexed
                    if not already_indexed and 'data' in result_data and result_data['data']:
                        try:
                            print("Storing embeddings to OpenSearch...")
                            storage_result = store_embeddings_to_opensearch(response, result_data['data'], original_s3_uri=original_s3_uri, user_id=embed_user)
                            print(f"OpenSearch storage result: {storage_result}")
                        except Exception as e:
                            print(f"Failed to store embeddings: {e}")
                            storage_result = {'error': str(e)}

                    # Return minimal data to avoid 413 error
                    segments_count = len(result_data.get('data', [])) if 'data' in result_data else 0

                    # Update embedding status in DDB
                    try:
                        embed_user_id = get_user_id(event)
                        vid_id = storage_result.get('video_id', 'unknown') if isinstance(storage_result, dict) else 'unknown'
                        table = get_metadata_table()
                        if table and vid_id != 'unknown':
                            response_ddb = table.query(
                                KeyConditionExpression='userId = :uid AND begins_with(sortKey, :prefix)',
                                ExpressionAttributeValues={':uid': embed_user_id, ':prefix': 'EMBEDDING#'},
                            )
                            for item in response_ddb.get('Items', []):
                                if item.get('invocationArn') == invocation_arn:
                                    table.update_item(
                                        Key={'userId': embed_user_id, 'sortKey': item['sortKey']},
                                        UpdateExpression='SET #s = :s, segmentsCount = :sc, completedAt = :t, completedAtISO = :iso',
                                        ExpressionAttributeNames={'#s': 'status'},
                                        ExpressionAttributeValues={
                                            ':s': 'completed',
                                            ':sc': segments_count,
                                            ':t': int(time.time()),
                                            ':iso': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                                        }
                                    )
                                    break
                    except Exception as e:
                        print(f"Failed to update embedding status in DDB: {e}")

                    return {
                        'statusCode': 200,
                        'headers': cors_headers,
                        'body': json.dumps({
                            'status': status,
                            'segments_processed': segments_count,
                            'opensearch_stored': storage_result.get('stored_count', 0) if isinstance(storage_result, dict) else 0,
                            'video_id': storage_result.get('video_id', 'unknown') if isinstance(storage_result, dict) else 'unknown',
                            'message': f'Embedding completed with {segments_count} segments stored'
                        })
                    }
                except Exception as e:
                    return {
                        'statusCode': 200,
                        'headers': cors_headers,
                        'body': json.dumps({
                            'status': status,
                            'message': f'Completed but could not retrieve result: {str(e)}'
                        })
                    }
            else:
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'status': status,
                        'message': 'Completed but no output S3 URI found in response'
                    })
                }
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'status': status,
                'message': f'Invocation is {status.lower()}'
            })
        }
    
    except ClientError as e:
        print(f"AWS ClientError in status: {e}")
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        print(f"Error code: {error_code}, Message: {error_message}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'AWS Error ({error_code}): {error_message}'})
        }
    except Exception as e:
        print(f"Unexpected error in status check: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f'Status check failed: {str(e)}'})
        }

def handle_search(event: Dict[str, Any], cors_headers: Dict[str, str]) -> Dict[str, Any]:
    """Handle vector similarity search"""
    limit_err = check_and_increment_usage(get_user_id(event), 'searchCount', cors_headers)
    if limit_err:
        return limit_err
    try:
        print("Starting search request...")
        query_params = event.get('queryStringParameters', {}) or {}
        query_text = query_params.get('q', '')
        print(f"Search query: {query_text}")
        
        if not query_text:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Query parameter q is required'})
            }
        
        # Generate embedding for query text using Marengo 3.0
        model_input = {
            "inputType": "text",
            "text": {
                "inputText": query_text
            }
        }
        
        try:
            user_id = get_user_id(event)
            print("Starting async query embedding generation...")
            response = bedrock_client.start_async_invoke(
                modelId='twelvelabs.marengo-embed-3-0-v1:0',
                modelInput=model_input,
                outputDataConfig={
                    's3OutputDataConfig': {
                        's3Uri': f"s3://{os.environ.get('VIDEO_BUCKET')}/search-embeddings/{user_id}/"
                    }
                }
            )
            
            invocation_arn = response.get('invocationArn')
            print(f"Started async embedding with ARN: {invocation_arn}")
            
            # Poll for completion (max 30 seconds for Lambda timeout)
            import time
            max_wait = 25  # seconds
            poll_interval = 1  # second
            waited = 0
            
            while waited < max_wait:
                status_response = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
                status = status_response.get('status')
                print(f"Embedding status: {status} (waited {waited}s)")
                
                if status == 'Completed':
                    # Get the result
                    output_data_config = status_response.get('outputDataConfig', {})
                    s3_output_config = output_data_config.get('s3OutputDataConfig', {})
                    output_s3_uri = s3_output_config.get('s3Uri')
                    
                    if output_s3_uri:
                        uri_parts = output_s3_uri.replace('s3://', '').split('/')
                        bucket = uri_parts[0]
                        key = '/'.join(uri_parts[1:]) + '/output.json'
                        
                        s3_response = s3_client.get_object(Bucket=bucket, Key=key)
                        result_data = json.loads(s3_response['Body'].read())
                        
                        if 'data' in result_data and result_data['data'] and 'embedding' in result_data['data'][0]:
                            query_embedding = result_data['data'][0]['embedding']
                            print(f"Retrieved query embedding length: {len(query_embedding)}")
                            break
                    
                elif status in ['Failed', 'Cancelled']:
                    raise Exception(f"Embedding generation {status.lower()}")
                
                time.sleep(poll_interval)
                waited += poll_interval
            
            if waited >= max_wait:
                return {
                    'statusCode': 408,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Query embedding generation timed out'})
                }
            
            if not query_embedding:
                return {
                    'statusCode': 500,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Failed to generate query embedding'})
                }
            
        except Exception as e:
            print(f"Failed to generate embedding: {e}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Failed to generate embedding: {str(e)}'})
            }
        
        # Search OpenSearch only
        try:
            search_result = search_opensearch(query_embedding, top_k=10, user_id=get_user_id(event))
        except Exception as e:
            search_result = {
                'results': [],
                'total': 0,
                'search_time_ms': 0,
                'error': str(e)
            }

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'results': search_result.get('results', []),
                'total': search_result.get('total', 0),
                'search_time_ms': search_result.get('search_time_ms', 0),
                'query': query_text,
                'message': search_result.get('message', f'Found {search_result.get("total", 0)} results')
            })
        }
    
    except Exception as e:
        print(f"Search handler failed: {e}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': str(e)})
        }

def handle_list_videos(event, cors_headers):
    """List all uploaded videos for the current user, plus shared videos"""
    try:
        user_id = get_user_id(event)
        table = get_metadata_table()
        if not table:
            return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': 'Metadata table not configured'})}

        def query_videos(uid, is_shared):
            resp = table.query(
                KeyConditionExpression='userId = :uid AND begins_with(sortKey, :prefix)',
                ExpressionAttributeValues={':uid': uid, ':prefix': 'VIDEO#'},
                ScanIndexForward=False,
            )
            out = []
            for item in resp.get('Items', []):
                out.append({
                    'key': item.get('key', ''),
                    'filename': item.get('filename', ''),
                    's3Uri': item.get('s3Uri', ''),
                    'bucket': item.get('bucket', ''),
                    'contentType': item.get('contentType', ''),
                    'uploadedAt': item.get('uploadedAtISO', ''),
                    'isShared': is_shared,
                })
            return out

        videos = query_videos(user_id, False)
        if user_id != SHARED_USER_ID:
            videos.extend(query_videos(SHARED_USER_ID, True))

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'videos': videos}, default=decimal_default)
        }
    except Exception as e:
        print(f"Error listing videos: {e}")
        return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': str(e)})}

def handle_index_samples(event, cors_headers):
    """Admin-only: enumerate sample videos and kick off a serial embedding chain.
    Only the first sample gets invoked; each Lambda chains into the next after
    finishing, so only one embedding runs at a time."""
    if not is_admin(event):
        return {
            'statusCode': 403,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Admin only'})
        }
    try:
        bucket_name = os.environ.get('VIDEO_BUCKET')
        prefix = 'videos/samples/'
        paginator = s3_client.get_paginator('list_objects_v2')
        sample_keys = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key == prefix or not key.lower().endswith('.mp4'):
                    continue
                filename = key[len(prefix):]
                if '/' in filename:
                    continue
                sample_keys.append(filename)
        sample_keys.sort()

        table = get_metadata_table()
        for filename in sample_keys:
            dest_key = f'videos/{SHARED_USER_ID}/{filename}'
            try:
                s3_client.head_object(Bucket=bucket_name, Key=dest_key)
            except ClientError:
                s3_client.copy_object(
                    Bucket=bucket_name,
                    Key=dest_key,
                    CopySource={'Bucket': bucket_name, 'Key': f'{prefix}{filename}'},
                    MetadataDirective='COPY',
                )
            if table:
                try:
                    table.put_item(Item={
                        'userId': SHARED_USER_ID,
                        'sortKey': f'VIDEO#{dest_key}',
                        'filename': filename,
                        's3Uri': f's3://{bucket_name}/{dest_key}',
                        'bucket': bucket_name,
                        'key': dest_key,
                        'contentType': 'video/mp4',
                        'uploadedAt': int(time.time()),
                        'uploadedAtISO': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                        'source': 'sample',
                    })
                except Exception as e:
                    print(f"DDB put for shared video failed: {e}")

        if not sample_keys:
            return {'statusCode': 200, 'headers': cors_headers, 'body': json.dumps({'message': 'No samples to index'})}

        lambda_client = boto3.client('lambda', region_name=os.environ.get('REGION', 'ap-northeast-2'))
        function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
        first, rest = sample_keys[0], sample_keys[1:]
        payload = {
            'internalAction': 'embed_shared_sample',
            's3Uri': f's3://{bucket_name}/videos/{SHARED_USER_ID}/{first}',
            'videoId': first,
            'queue': rest,
        }
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload).encode('utf-8'),
        )

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Started serial embedding chain: {first} then {len(rest)} more',
                'samples': sample_keys,
            })
        }
    except Exception as e:
        print(f"index-samples failed: {e}")
        import traceback
        print(traceback.format_exc())
        return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': str(e)})}

def process_shared_sample_embedding(s3_uri: str, video_id: str, queue=None):
    """Run synchronously inside an async-invoked Lambda: start Marengo embedding,
    poll until it completes, and persist results to OpenSearch under SHARED_USER_ID.
    When finished, kick off the next sample in `queue` to keep processing serial."""
    print(f"[shared-embed] starting for {s3_uri}  queue_left={len(queue or [])}")

    safe_video_id = video_id.rsplit('.', 1)[0] if '.' in video_id else video_id
    safe_video_id = safe_video_id.replace('/', '_').replace(' ', '_')

    model_input = {
        "inputType": "video",
        "video": {
            "mediaSource": {
                "s3Location": {
                    "uri": s3_uri,
                    "bucketOwner": get_account_id()
                }
            },
            "segmentation": {
                "method": "fixed",
                "fixed": {"durationSec": 10}
            },
            "embeddingOption": ["visual", "audio"],
            "embeddingScope": ["clip"]
        }
    }

    bucket_name = os.environ.get('VIDEO_BUCKET')
    response = bedrock_client.start_async_invoke(
        modelId='twelvelabs.marengo-embed-3-0-v1:0',
        modelInput=model_input,
        outputDataConfig={
            's3OutputDataConfig': {
                's3Uri': f"s3://{bucket_name}/embeddings/{SHARED_USER_ID}/{safe_video_id}/"
            }
        }
    )
    invocation_arn = response.get('invocationArn')
    print(f"[shared-embed] arn={invocation_arn}")

    table = get_metadata_table()
    if table:
        try:
            table.put_item(Item={
                'userId': SHARED_USER_ID,
                'sortKey': f'EMBEDDING#{video_id}',
                'videoId': video_id,
                's3Uri': s3_uri,
                'invocationArn': invocation_arn,
                'status': 'processing',
                'createdAt': int(time.time()),
                'createdAtISO': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            })
        except Exception as e:
            print(f"[shared-embed] DDB embed record failed: {e}")

    max_wait = 14 * 60
    waited = 0
    output_s3_uri = None
    status = None
    while waited < max_wait:
        status_resp = bedrock_client.get_async_invoke(invocationArn=invocation_arn)
        status = status_resp.get('status')
        print(f"[shared-embed] status={status} waited={waited}s")
        if status == 'Completed':
            output_s3_uri = status_resp.get('outputDataConfig', {}).get('s3OutputDataConfig', {}).get('s3Uri')
            break
        if status in ('Failed', 'Cancelled'):
            raise RuntimeError(f"Marengo embedding {status}")
        time.sleep(10)
        waited += 10

    if not output_s3_uri:
        raise RuntimeError('Embedding timed out')

    uri_parts = output_s3_uri.replace('s3://', '').split('/')
    out_bucket = uri_parts[0]
    out_key = '/'.join(uri_parts[1:]) + '/output.json'
    result_data = json.loads(s3_client.get_object(Bucket=out_bucket, Key=out_key)['Body'].read())

    if 'data' in result_data and result_data['data']:
        store_embeddings_to_opensearch(
            status_resp,
            result_data['data'],
            original_s3_uri=s3_uri,
            user_id=SHARED_USER_ID,
        )
        print(f"[shared-embed] stored {len(result_data['data'])} segments for {video_id}")

    if table:
        try:
            table.update_item(
                Key={'userId': SHARED_USER_ID, 'sortKey': f'EMBEDDING#{video_id}'},
                UpdateExpression='SET #s = :s, completedAt = :t',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':s': 'completed', ':t': int(time.time())},
            )
        except Exception as e:
            print(f"[shared-embed] DDB status update failed: {e}")

    if queue:
        next_filename = queue[0]
        rest = queue[1:]
        try:
            lambda_client = boto3.client('lambda', region_name=os.environ.get('REGION', 'ap-northeast-2'))
            lambda_client.invoke(
                FunctionName=os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),
                InvocationType='Event',
                Payload=json.dumps({
                    'internalAction': 'embed_shared_sample',
                    's3Uri': f"s3://{os.environ.get('VIDEO_BUCKET')}/videos/{SHARED_USER_ID}/{next_filename}",
                    'videoId': next_filename,
                    'queue': rest,
                }).encode('utf-8'),
            )
            print(f"[shared-embed] chained next={next_filename} remaining={len(rest)}")
        except Exception as e:
            print(f"[shared-embed] chain invoke failed: {e}")

def handle_list_analyses(event, cors_headers):
    """List analysis history for a video"""
    try:
        user_id = get_user_id(event)
        query_params = event.get('queryStringParameters', {}) or {}
        video_id = query_params.get('videoId', '')

        table = get_metadata_table()
        if not table:
            return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': 'Metadata table not configured'})}

        if video_id:
            prefix = f'ANALYSIS#{video_id}'
        else:
            prefix = 'ANALYSIS#'

        response = table.query(
            KeyConditionExpression='userId = :uid AND begins_with(sortKey, :prefix)',
            ExpressionAttributeValues={':uid': user_id, ':prefix': prefix},
            ScanIndexForward=False,
        )

        analyses = []
        for item in response.get('Items', []):
            analyses.append({
                'jobId': item.get('jobId', ''),
                'videoId': item.get('videoId', ''),
                'prompt': item.get('prompt', ''),
                'status': item.get('status', ''),
                'analysis': item.get('analysis', ''),
                'error': item.get('error', ''),
                'createdAt': item.get('createdAtISO', ''),
                'completedAt': item.get('completedAtISO', ''),
            })

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'analyses': analyses}, default=decimal_default)
        }
    except Exception as e:
        print(f"Error listing analyses: {e}")
        return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': str(e)})}

def handle_list_embeddings(event, cors_headers):
    """List embedding statuses for all user's videos"""
    try:
        user_id = get_user_id(event)
        table = get_metadata_table()
        if not table:
            return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': 'Metadata table not configured'})}

        response = table.query(
            KeyConditionExpression='userId = :uid AND begins_with(sortKey, :prefix)',
            ExpressionAttributeValues={':uid': user_id, ':prefix': 'EMBEDDING#'},
            ScanIndexForward=False,
        )

        embeddings = []
        for item in response.get('Items', []):
            embeddings.append({
                'videoId': item.get('videoId', ''),
                's3Uri': item.get('s3Uri', ''),
                'status': item.get('status', ''),
                'invocationArn': item.get('invocationArn', ''),
                'segmentsCount': int(item.get('segmentsCount', 0)),
                'createdAt': item.get('createdAtISO', ''),
                'completedAt': item.get('completedAtISO', ''),
            })

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'embeddings': embeddings}, default=decimal_default)
        }
    except Exception as e:
        print(f"Error listing embeddings: {e}")
        return {'statusCode': 500, 'headers': cors_headers, 'body': json.dumps({'error': str(e)})}
# deploy test
