# Video Understanding PoC with Twelve Labs Models

A proof-of-concept application that leverages Amazon Bedrock's Twelve Labs models (Marengo and Pegasus) for video understanding, embedding generation, and semantic search.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   React App     │    │   API Gateway   │    │   Lambda        │
│   (Frontend)    │───▶│                 │───▶│   (Python)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                       ┌─────────────────┐             │
                       │   S3 Bucket     │◀────────────┤
                       │   (Videos)      │             │
                       └─────────────────┘             │
                                                        │
                       ┌─────────────────┐             │
                       │   OpenSearch    │◀────────────┤
                       │   Serverless    │             │
                       │   (Vectors)     │             │
                       └─────────────────┘             │
                                                        │
                       ┌─────────────────┐             │
                       │   Bedrock       │◀────────────┘
                       │   Twelve Labs   │
                       │   Models        │
                       └─────────────────┘
```

## Features

### 🎥 Video Upload
- Multipart upload to S3
- Support for MP4, MOV, AVI formats
- Up to 2GB file size, 2 hours duration
- Drag-and-drop interface

### 🧠 Video Analysis (Twelve Labs Pegasus)
- Comprehensive video understanding
- Custom prompt-based analysis
- Textual descriptions and insights
- Scene and action recognition

### 🔍 Embedding Generation (Twelve Labs Marengo)
- Visual-text embeddings
- Visual-image embeddings  
- Audio embeddings
- Asynchronous processing

### 🔎 Semantic Search
- Vector similarity search
- Natural language queries
- OpenSearch Serverless backend
- Ranked results by similarity

## Technology Stack

- **Frontend**: React 18 + TypeScript
- **Backend**: Python 3.11 + AWS Lambda
- **Infrastructure**: AWS CDK (TypeScript)
- **Vector Database**: OpenSearch Serverless
- **Storage**: Amazon S3
- **AI Models**: Twelve Labs Marengo & Pegasus via Bedrock
- **Region**: Configurable (defaults to us-east-1 for S3 Vectors support)

## Prerequisites

- AWS CLI configured with appropriate permissions
- Node.js 18+ and npm
- Python 3.11+
- AWS CDK CLI (`npm install -g aws-cdk`)

## Required AWS Permissions

Your AWS user/role needs permissions for:
- Bedrock model access (Twelve Labs models)
- S3 bucket creation and operations
- OpenSearch Serverless
- Lambda function deployment
- API Gateway
- CloudFormation
- IAM role creation

## Enablem Twelve Labs model on Bedrock
- Before Deployment, you should enable (opt-in) for Twelve Labs model on Amazon Bedrock console 

## Quick Start

1. **Clone and setup**:
   ```bash
   git clone <repository>
   cd TwelvelLabs-on-bedrock-demo
   ```

2. **Configure your environment** (optional):
   ```bash
   # Set your preferred region (defaults to us-east-1)
   export AWS_DEFAULT_REGION=us-east-1
   
   # Set CORS origins for your frontend domain (optional)
   export CORS_ORIGINS=http://localhost:3000,https://yourdomain.com
   ```

3. **Deploy infrastructure**:
   ```bash
   ./deploy.sh
   ```

4. **Start frontend**:
   ```bash
   cd frontend
   npm install
   npm start
   ```

5. **Access the application**:
   - Open http://localhost:3000
   - Upload a video
   - Analyze with Pegasus
   - Generate embeddings with Marengo
   - Search for similar content

## Manual Deployment

If you prefer manual deployment:

### 1. Deploy Infrastructure
```bash
# Set your preferred region
export AWS_DEFAULT_REGION=us-east-1
export CDK_DEFAULT_REGION=$AWS_DEFAULT_REGION

cd infrastructure
npm install
npx cdk bootstrap --region $AWS_DEFAULT_REGION
npx cdk deploy
```

### 2. Configure Frontend
```bash
cd frontend
npm install

# Get API Gateway URL from CloudFormation outputs
API_URL=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack \
  --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text \
  --region $AWS_DEFAULT_REGION)

# Create .env file
echo "REACT_APP_API_URL=$API_URL" > .env

npm start
```

## Environment Configuration

The application supports dynamic configuration through environment variables:

### Deployment Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_DEFAULT_REGION` | AWS region for deployment | `us-east-1` |
| `CDK_DEFAULT_REGION` | CDK deployment region | `$AWS_DEFAULT_REGION` |
| `CORS_ORIGINS` | Allowed CORS origins (comma-separated) | `http://localhost:3000,https://localhost:3000` |

### Runtime Environment Variables (Lambda)

| Variable | Description | Auto-set by CDK |
|----------|-------------|-----------------|
| `VIDEO_BUCKET` | S3 bucket name for videos | ✅ |
| `OPENSEARCH_ENDPOINT` | OpenSearch collection endpoint | ✅ |
| `REGION` | AWS region | ✅ |
| `AWS_ACCOUNT_ID` | AWS account ID | ✅ |
| `CORS_ORIGIN` | Single CORS origin for Lambda responses | ✅ |

### Test Environment Variables

After running `./deploy.sh`, these are automatically set in `.env.test`:

| Variable | Description |
|----------|-------------|
| `API_BASE_URL` | API Gateway URL |
| `AWS_ACCOUNT_ID` | Your AWS account ID |
| `AWS_REGION` | Deployment region |

### Frontend Environment Variables

Set in `frontend/.env`:

| Variable | Description |
|----------|-------------|
| `REACT_APP_API_URL` | API Gateway URL |

## API Endpoints

### POST /upload
Get presigned URL for video upload to S3.

**Request**:
```json
{
  "filename": "video.mp4",
  "contentType": "video/mp4"
}
```

**Response**:
```json
{
  "uploadUrl": "https://s3-presigned-url",
  "key": "videos/video.mp4",
  "bucket": "bucket-name"
}
```

### POST /analyze
Analyze video using Twelve Labs Pegasus.

**Request**:
```json
{
  "s3Uri": "s3://bucket/videos/video.mp4",
  "prompt": "Analyze this video and describe what you see"
}
```

**Response**:
```json
{
  "analysis": "This video shows...",
  "finishReason": "stop"
}
```

### POST /embed
Generate embeddings using Twelve Labs Marengo.

**Request**:
```json
{
  "s3Uri": "s3://bucket/videos/video.mp4",
  "videoId": "unique-video-id"
}
```

**Response**:
```json
{
  "invocationArn": "arn:aws:bedrock:...",
  "status": "processing",
  "message": "Embedding generation started"
}
```

### GET /search?q=query
Search videos by content similarity.

**Response**:
```json
{
  "results": [
    {
      "videoId": "video-id",
      "score": 0.95,
      "metadata": {}
    }
  ],
  "total": 1
}
```

## Model Information

### Twelve Labs Pegasus 1.2
- **Model ID**: `twelvelabs.pegasus-1-2-v1:0`
- **Purpose**: Video understanding and analysis
- **Input**: Video (up to 1 hour, <2GB)
- **Output**: Text analysis and descriptions
- **API**: `InvokeModel` (synchronous)

### Twelve Labs Marengo Embed 2.7
- **Model ID**: `twelvelabs.marengo-embed-2-7-v1:0`
- **Purpose**: Video embedding generation
- **Input**: Video (up to 2 hours, <2GB)
- **Output**: Vector embeddings (visual-text, visual-image, audio)
- **API**: `StartAsyncInvoke` (asynchronous)

## Troubleshooting

### Region-Specific Deployment

```bash
# Deploy to us-west-2
export AWS_DEFAULT_REGION=us-west-2
./deploy.sh

# Deploy to eu-west-1 
export AWS_DEFAULT_REGION=eu-west-1
./deploy.sh

# Deploy with custom CORS origins
export AWS_DEFAULT_REGION=us-east-1
export CORS_ORIGINS=https://mydomain.com,https://app.mydomain.com
./deploy.sh
```

**Note**: S3 Vectors service is only available in specific regions. The deploy script will warn you if deploying to an unsupported region.

### Common Issues

1. **Bedrock Model Access**:
   - Ensure Twelve Labs models are enabled in your AWS region
   - Check IAM permissions for Bedrock access
   - Verify region supports Twelve Labs models

2. **Region Support**:
   - S3 Vectors: Limited region availability
   - Twelve Labs models: Check Bedrock model availability
   - Some regions may not support all features

3. **Upload Failures**:
   - Verify S3 bucket permissions
   - Check file size limits (2GB max)
   - Ensure CORS is properly configured

4. **Search Returns No Results**:
   - Ensure videos have been processed with embeddings
   - Check OpenSearch Serverless configuration
   - Verify vector index creation

5. **Lambda Timeouts**:
   - Video processing can take time
   - Consider increasing Lambda timeout for large videos
   - Monitor CloudWatch logs for errors

### Logs and Monitoring

- **Lambda Logs**: CloudWatch Logs
- **API Gateway**: CloudWatch Logs and X-Ray
- **Frontend**: Browser Developer Tools

## Cost Considerations

- **Bedrock**: Pay per token/request
- **OpenSearch Serverless**: Pay for OCU usage
- **S3**: Storage and transfer costs
- **Lambda**: Execution time and memory
- **API Gateway**: Request-based pricing

## Security Notes

- S3 bucket has CORS enabled for frontend access
- Lambda functions have minimal IAM permissions
- OpenSearch collection uses IAM authentication
- No hardcoded credentials in code

## Future Enhancements

- [ ] Video thumbnail generation
- [ ] Batch processing for multiple videos
- [ ] Advanced search filters
- [ ] Video player integration
- [ ] Embedding visualization
- [ ] Real-time processing status
- [ ] User authentication
- [ ] Video metadata extraction

## Contributing

This is a PoC project. For production use, consider:
- Error handling improvements
- Input validation
- Rate limiting
- Monitoring and alerting
- Security hardening
- Performance optimization

## License

MIT License - see LICENSE file for details.
