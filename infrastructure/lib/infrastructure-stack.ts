import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as opensearchserverless from 'aws-cdk-lib/aws-opensearchserverless';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as amplify from 'aws-cdk-lib/aws-amplify';
import { Construct } from 'constructs';
import * as path from 'path';

export class InfrastructureStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for video storage
    const videoBucket = new s3.Bucket(this, 'VideoBucket', {
      bucketName: `video-understanding-${this.account}-${this.region}`,
      cors: [
        {
          allowedHeaders: ['*'],
          allowedMethods: [
            s3.HttpMethods.GET,
            s3.HttpMethods.POST,
            s3.HttpMethods.PUT,
            s3.HttpMethods.DELETE,
            s3.HttpMethods.HEAD,
          ],
          allowedOrigins: ['*'],
          exposedHeaders: ['ETag'],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // DynamoDB metadata table
    const metadataTable = new dynamodb.Table(this, 'MetadataTable', {
      tableName: `video-understanding-metadata-${this.region}`,
      partitionKey: { name: 'userId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sortKey', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Cognito User Pool (L1 construct to ensure AllowAdminCreateUserOnly is false)
    const cfnUserPool = new cognito.CfnUserPool(this, 'VideoUserPoolCfn', {
      userPoolName: `video-understanding-users-${this.region}`,
      adminCreateUserConfig: {
        allowAdminCreateUserOnly: false,
      },
      autoVerifiedAttributes: ['email'],
      usernameAttributes: ['email'],
      policies: {
        passwordPolicy: {
          minimumLength: 8,
          requireLowercase: true,
          requireUppercase: true,
          requireNumbers: true,
          requireSymbols: false,
        },
      },
      accountRecoverySetting: {
        recoveryMechanisms: [{ name: 'verified_email', priority: 1 }],
      },
      schema: [{
        name: 'email',
        attributeDataType: 'String',
        required: true,
        mutable: true,
      }],
    });
    cfnUserPool.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    const userPool = cognito.UserPool.fromUserPoolId(this, 'VideoUserPool', cfnUserPool.ref);

    const cfnUserPoolClient = new cognito.CfnUserPoolClient(this, 'VideoUserPoolClientCfn', {
      userPoolId: cfnUserPool.ref,
      clientName: 'video-understanding-web',
      explicitAuthFlows: ['ALLOW_USER_SRP_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
      preventUserExistenceErrors: 'ENABLED',
    });

    const userPoolClient = cognito.UserPoolClient.fromUserPoolClientId(this, 'VideoUserPoolClient', cfnUserPoolClient.ref);

    // OpenSearch Serverless encryption policy
    const encryptionPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'VideoEmbeddingsCollectionEncryptionPolicy', {
      name: 'encryptionpolicyvidetionb7b973ac',
      type: 'encryption',
      policy: JSON.stringify({
        Rules: [{
          ResourceType: 'collection',
          Resource: ['collection/video-embeddings']
        }],
        AWSOwnedKey: true
      }),
    });

    // OpenSearch Serverless network policy
    const networkPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'VideoEmbeddingsCollectionNetworkPolicy', {
      name: 'networkpolicyvideoctionb7b973ac',
      type: 'network',
      policy: JSON.stringify([{
        Rules: [{
          ResourceType: 'collection',
          Resource: ['collection/video-embeddings']
        }, {
          ResourceType: 'dashboard',
          Resource: ['collection/video-embeddings']
        }],
        AllowFromPublic: true
      }]),
    });

    // OpenSearch Serverless collection
    const vectorCollection = new opensearchserverless.CfnCollection(this, 'VideoEmbeddingsCollectionVectorCollection', {
      name: 'video-embeddings',
      description: 'Vector collection for video embeddings from Twelve Labs Marengo',
      type: 'VECTORSEARCH',
      standbyReplicas: 'ENABLED',
      tags: [{
        key: 'Name',
        value: 'video-embeddings',
      }, {
        key: 'Type',
        value: 'VectorCollection',
      }],
    });
    vectorCollection.addDependency(encryptionPolicy);
    vectorCollection.addDependency(networkPolicy);

    // IAM managed policy for OpenSearch access
    const aossApiAccessPolicy = new iam.ManagedPolicy(this, 'VideoEmbeddingsCollectionAOSSApiAccessAll', {
      statements: [
        new iam.PolicyStatement({
          actions: ['aoss:APIAccessAll'],
          resources: [vectorCollection.attrArn],
        }),
      ],
    });

    // Lambda execution role
    const lambdaRole = new iam.Role(this, 'VideoProcessingLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        aossApiAccessPolicy,
      ],
      inlinePolicies: {
        BedrockAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'bedrock:GetAsyncInvoke',
                'bedrock:InvokeModel',
                'bedrock:InvokeModelWithResponseStream',
                'bedrock:StartAsyncInvoke',
              ],
              resources: [
                'arn:aws:bedrock:*::foundation-model/twelvelabs.*',
                `arn:aws:bedrock:*:${this.account}:async-invoke/*`,
                `arn:aws:bedrock:*:${this.account}:inference-profile/us.twelvelabs.*`,
              ],
            }),
          ],
        }),
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                's3:DeleteObject',
                's3:GetObject',
                's3:PutObject',
              ],
              resources: [videoBucket.arnForObjects('*')],
            }),
            new iam.PolicyStatement({
              actions: ['s3:ListBucket'],
              resources: [videoBucket.bucketArn],
            }),
          ],
        }),
        OpenSearchAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'aoss:APIAccessAll',
                'aoss:CreateIndex',
                'aoss:DeleteIndex',
              ],
              resources: [vectorCollection.attrArn],
            }),
          ],
        }),
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'dynamodb:GetItem',
                'dynamodb:PutItem',
                'dynamodb:UpdateItem',
                'dynamodb:Query',
              ],
              resources: [metadataTable.tableArn],
            }),
          ],
        }),
        LambdaInvokeAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ['lambda:InvokeFunction'],
              resources: [`arn:aws:lambda:${this.region}:${this.account}:function:*`],
            }),
          ],
        }),
      },
    });

    // OpenSearch data access policy
    const dataAccessPolicy = new opensearchserverless.CfnAccessPolicy(this, 'VideoEmbeddingsCollectionDataAccessPolicy', {
      name: 'dataaccesspolicyvidetionb7b973ac',
      type: 'data',
      policy: JSON.stringify([{
        Rules: [{
          Resource: ['collection/video-embeddings'],
          Permission: [
            'aoss:DescribeCollectionItems',
            'aoss:CreateCollectionItems',
            'aoss:UpdateCollectionItems',
          ],
          ResourceType: 'collection',
        }, {
          Resource: ['index/video-embeddings/*'],
          Permission: [
            'aoss:UpdateIndex',
            'aoss:DescribeIndex',
            'aoss:ReadDocument',
            'aoss:WriteDocument',
            'aoss:CreateIndex',
            'aoss:DeleteIndex',
          ],
          ResourceType: 'index',
        }],
        Principal: [lambdaRole.roleArn],
        Description: '',
      }]),
    });

    // Lambda function
    const videoProcessingFunction = new lambda.Function(this, 'VideoProcessingFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'main.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../backend'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          local: {
            tryBundle(outputDir: string) {
              try {
                const { execSync } = require('child_process');
                const backendPath = path.join(__dirname, '../../backend');
                
                // Install dependencies locally
                execSync(`pip install -r requirements.txt -t ${outputDir}`, {
                  cwd: backendPath,
                  stdio: 'inherit',
                });
                
                // Copy source files
                execSync(`cp -r ${backendPath}/* ${outputDir}/`, {
                  stdio: 'inherit',
                });
                
                return true;
              } catch (error) {
                console.log('Local bundling failed, falling back to Docker:', error);
                return false;
              }
            },
          },
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output'
          ],
        },
      }),
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      role: lambdaRole,
      environment: {
        VIDEO_BUCKET: videoBucket.bucketName,
        OPENSEARCH_ENDPOINT: vectorCollection.attrCollectionEndpoint,
        REGION: this.region,
        AWS_ACCOUNT_ID: this.account,

        METADATA_TABLE: metadataTable.tableName,
        CORS_ORIGIN: '*',
        ADMIN_USER_SUBS: 'f4983d7c-2031-70ec-dfda-38beab6cffc7',
      },
    });


    // API Gateway
    const api = new apigateway.RestApi(this, 'VideoUnderstandingApi', {
      restApiName: 'Video Understanding API',
      description: 'API for video understanding using Twelve Labs models',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: [
          'Content-Type',
          'X-Amz-Date',
          'Authorization',
          'X-Api-Key',
          'X-Amz-Security-Token',
        ],
      },
    });

    const authorizer = new apigateway.CognitoUserPoolsAuthorizer(this, 'CognitoAuthorizer', {
      cognitoUserPools: [userPool],
    });

    const integration = new apigateway.LambdaIntegration(videoProcessingFunction);

    // Add API methods
    api.root.addResource('upload').addMethod('POST', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('analyze').addMethod('POST', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('embed').addMethod('POST', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('search').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('status').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('video-url').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('flush-opensearch').addMethod('POST', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('videos').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('analyses').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    api.root.addResource('embeddings').addMethod('GET', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });
    const admin = api.root.addResource('admin');
    admin.addResource('index-samples').addMethod('POST', integration, {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    });

    // Outputs
    new cdk.CfnOutput(this, 'ApiUrl', {
      description: 'API Gateway URL',
      value: api.url,
    });

    new cdk.CfnOutput(this, 'VideoBucketName', {
      description: 'S3 bucket for video storage',
      value: videoBucket.bucketName,
    });

    new cdk.CfnOutput(this, 'OpenSearchEndpoint', {
      description: 'OpenSearch Serverless collection endpoint',
      value: vectorCollection.attrCollectionEndpoint,
    });


    new cdk.CfnOutput(this, 'MetadataTableName', {
      description: 'DynamoDB metadata table name',
      value: metadataTable.tableName,
    });

    // Amplify Hosting
    const amplifyApp = new amplify.CfnApp(this, 'FrontendApp', {
      name: `video-understanding-${this.account}-${this.region}`,
      platform: 'WEB',
    });

    const mainBranch = new amplify.CfnBranch(this, 'MainBranch', {
      appId: amplifyApp.attrAppId,
      branchName: 'main',
    });

    new cdk.CfnOutput(this, 'UserPoolId', {
      description: 'Cognito User Pool ID',
      value: userPool.userPoolId,
    });

    new cdk.CfnOutput(this, 'UserPoolClientId', {
      description: 'Cognito User Pool Client ID',
      value: userPoolClient.userPoolClientId,
    });

    new cdk.CfnOutput(this, 'AmplifyAppId', {
      description: 'Amplify App ID',
      value: amplifyApp.attrAppId,
    });

    new cdk.CfnOutput(this, 'AmplifyDefaultDomain', {
      description: 'Amplify Default Domain',
      value: `https://main.${amplifyApp.attrAppId}.amplifyapp.com`,
    });
  }
}
