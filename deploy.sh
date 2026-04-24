#!/bin/bash

# Video Understanding PoC Deployment Script

set -e

echo "🚀 Starting deployment of Video Understanding PoC..."

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

# Set region from environment or default to ap-northeast-2 (Seoul)
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-ap-northeast-2}
echo "📍 Using region: $AWS_DEFAULT_REGION"

# Deploy CDK infrastructure
echo "🏗️  Deploying CDK infrastructure..."
cd infrastructure

# Install dependencies
npm install

# Bootstrap CDK (if not already done)
npx cdk bootstrap --region $AWS_DEFAULT_REGION

# Deploy the stack
npx cdk deploy --require-approval never

# Get outputs
API_URL=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text --region $AWS_DEFAULT_REGION)
BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack --query 'Stacks[0].Outputs[?OutputKey==`VideoBucketName`].OutputValue' --output text --region $AWS_DEFAULT_REGION)
USER_POOL_ID=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack --query 'Stacks[0].Outputs[?OutputKey==`UserPoolId`].OutputValue' --output text --region $AWS_DEFAULT_REGION)
USER_POOL_CLIENT_ID=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack --query 'Stacks[0].Outputs[?OutputKey==`UserPoolClientId`].OutputValue' --output text --region $AWS_DEFAULT_REGION)
AMPLIFY_APP_ID=$(aws cloudformation describe-stacks --stack-name VideoUnderstandingStack --query 'Stacks[0].Outputs[?OutputKey==`AmplifyAppId`].OutputValue' --output text --region $AWS_DEFAULT_REGION)

echo "✅ Infrastructure deployed successfully!"
echo "📡 API URL: $API_URL"
echo "🪣 S3 Bucket: $BUCKET_NAME"

# Build frontend with environment variables
cd ../frontend
npm install

REACT_APP_API_URL=$API_URL \
REACT_APP_USER_POOL_ID=$USER_POOL_ID \
REACT_APP_USER_POOL_CLIENT_ID=$USER_POOL_CLIENT_ID \
REACT_APP_REGION=$AWS_DEFAULT_REGION \
npm run build

# Deploy to Amplify
cd build
zip -r ../build.zip .
cd ..

DEPLOY_RESULT=$(aws amplify create-deployment --app-id $AMPLIFY_APP_ID --branch-name main --output json --region $AWS_DEFAULT_REGION)
UPLOAD_URL=$(echo $DEPLOY_RESULT | python3 -c "import sys, json; print(json.load(sys.stdin)['zipUploadUrl'])")
JOB_ID=$(echo $DEPLOY_RESULT | python3 -c "import sys, json; print(json.load(sys.stdin)['jobId'])")

curl --request PUT --upload-file build.zip "$UPLOAD_URL" --silent
aws amplify start-deployment --app-id $AMPLIFY_APP_ID --branch-name main --job-id $JOB_ID --region $AWS_DEFAULT_REGION

echo "🌐 Frontend URL: https://main.${AMPLIFY_APP_ID}.amplifyapp.com"

# Create environment configuration for testing
echo "🧪 Creating test environment configuration..."
cat > .env.test << EOF
API_BASE_URL=${API_URL}
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=${AWS_DEFAULT_REGION}
EOF

echo ""
echo "🎉 Deployment completed successfully!"
echo ""
echo "📚 API Endpoints:"
echo "   POST $API_URL/upload - Get presigned URL for video upload"
echo "   POST $API_URL/analyze - Analyze video with Pegasus"
echo "   POST $API_URL/embed - Generate embeddings with Marengo"
echo "   GET  $API_URL/search?q=query - Search videos by content"
echo ""
echo "🔧 Environment Configuration:"
echo "   API URL: $API_URL"
echo "   S3 Bucket: $BUCKET_NAME"
echo "   User Pool ID: $USER_POOL_ID"
echo "   Amplify App ID: $AMPLIFY_APP_ID"
echo "   Frontend URL: https://main.${AMPLIFY_APP_ID}.amplifyapp.com"
echo "   AWS Account: $(aws sts get-caller-identity --query Account --output text)"
echo "   Region: $AWS_DEFAULT_REGION"
