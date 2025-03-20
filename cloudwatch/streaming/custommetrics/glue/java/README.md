
Glue Job-Level Custom Metrics Lambda (Java)
=========================================

This is a comprehensive AWS Lambda function written in Java that retrieves and calculates seven custom Glue job-level metrics and five job run-level 
metrics, then publishes them to AWS CloudWatch under the namespace **'Broadcom/AwsCustomMetric'**:

### Glue Job-Level Metrics:
*   **Execution Count**
*   **Average Execution Duration (Sec)**
*   **Latest Run State**
*   **Latest Run Execution Duration (Sec)**
*   **Latest Run Start Time (Epoch)**
*   **Latest Run Completion Time (Epoch)**
*   **Latest Run Error State**

### Glue Job Run-Level Metrics:
*   **Current Run State**
*   **Run Start Time (Epoch)**
*   **Run Completion Time (Epoch)**
*   **Run Execution Duration (Sec)**
*   **Run Error State**
    

Required IAM Permissions
------------------------

The following IAM policy permissions are required for the Lambda function to operate correctly:


```json
  {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:CreateLogGroup",
        "logs:PutLogEvents",
        "CloudWatch:PutMetricData",
        "glue:ListJobs",
        "glue:GetJobRuns"
      ],
      "Resource": "*"
    }
  ]
}

```
Prerequisites
-----------------

Before building and deploying, ensure you have:

*   **Java 8** or later installed
    
*   **Maven 3.x**
    
*   AWS CLI (optional, for manual deployment)
    
*   An AWS IAM role with necessary permissions
    

Building & Packaging the Lambda Function
-------------------------------------------

To compile and package the Lambda function as a deployable JAR:

``` bash
  mvn clean package
```

This will generate a JAR file with all dependencies at:

``` bash
target/glue-custom-metrics-lambda-1.0.0-jar-with-dependencies.jar
```

Uploading to AWS Lambda
---------------------------


*   Go to AWS Lambda Console.
    
*   Click **Create Function** → **Author from Scratch**.
    
*   **Runtime**: Choose **Java 8 or later**.
    
*   com.yourcompany.GlueCustomMetricsLambda::handleRequest
    
*   **Upload JAR File**: Choose the jar-with-dependencies.jar generated above.

**Note:** Please ensure to change the AWS Lambda function timeout to 30 seconds or more (default is 15 seconds) to ensure sufficient execution time.
    

If you prefer command-line deployment:

``` bash
   aws lambda update-function-code \    --function-name GlueCustomMetricsLambda \    --zip-file fileb://target/glue-custom-metrics-lambda-1.0.0-jar-with-dependencies.jar  
```

    

Steps to Trigger Lambda Using EventBridge
-----------------------------------------

1.  **Navigate to EventBridge in the AWS Management Console**.
    
2.  **Create a new rule** with a schedule-based trigger (e.g., cron or fixed rate).
    
3.  **Select your Lambda function as the target** of the EventBridge rule.
    
4.  **Configure the required permissions** for EventBridge to invoke the Lambda function.
    
5.  **Save the rule** to enable automatic invocation of the Lambda based on your defined schedule.
    

After completing these steps, the Lambda function is ready to deploy, trigger, and execute to collect and publish the custom Glue job-level metrics.

