
Glue Job-Level Custom Metrics Lambda (Go)
=========================================

This is a comprehensive AWS Lambda function written in Java that retrieves and calculates seven custom Glue job-level metrics and five job run-level
metrics, then publishes them to AWS CloudWatch under the namespace 'Broadcom/AwsCustomMetric':

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
Steps to Build and Deploy Lambda Executable
-------------------------------------------

To build an executable for AWS Lambda on your local machine:

1.  **Build the executable** using the command below in a Git Bash terminal (or similar):
    

``` bash
     GOOS=linux GOARCH=arm64 go build -o bootstrap lambda.go
```	 

1.  **Package the executable**:
    

*  Zip the generated bootstrap file.

*  **Upload the zip package** to your AWS Lambda function.

**Note:** Please ensure to change the AWS Lambda function timeout to 30 seconds (default is 15 seconds) to ensure sufficient execution time.
    

Steps to Trigger Lambda Using EventBridge
-----------------------------------------

1.  **Navigate to EventBridge in the AWS Management Console**.
    
2.  **Create a new rule** with a schedule-based trigger (e.g., cron or fixed rate).
    
3.  **Select your Lambda function as the target** of the EventBridge rule.
    
4.  **Configure the required permissions** for EventBridge to invoke the Lambda function.
    
5.  **Save the rule** to enable automatic invocation of the Lambda based on your defined schedule.
    

After completing these steps, the Lambda function is ready to deploy, trigger, and execute to collect and publish the custom Glue job-level metrics.

