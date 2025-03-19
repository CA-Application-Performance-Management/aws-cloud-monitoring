package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	types2 "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"log"
	"time"
)

const AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME = "Execution Count"
const AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME = "Average Execution Duration (Sec)"
const AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME = "Latest Run State"
const AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME = "Latest Run Execution Duration (Sec)"
const AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME = "Latest Run Start Time (Epoch)"
const AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME = "Latest Run Completion Time (Epoch)"
const AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME = "Latest Run Error State"
const AWS_GLUE_JOB_RUN_CURRENT_STATE_CUSTOM_METRIC_NAME = "Current Run State"
const AWS_GLUE_JOB_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME = "Run Start Time (Epoch)"
const AWS_GLUE_JOB_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME = "Run Completion Time (Epoch)"
const AWS_GLUE_JOB_RUN_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME = "Run Execution Duration (Sec)"
const AWS_GLUE_JOB_RUN_ERROR_STATE_CUSTOM_METRIC_NAME = "Run Error State"

func main() {
	lambda.Start(lambdaHandler)
}

func lambdaHandler(ctx context.Context) {
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to Load AWS Config: %v", err)
	} else {
		// Initialize AWS service clients
		awsGlueClient := glue.NewFromConfig(awsConfig)
		awsCloudWatchClient := cloudwatch.NewFromConfig(awsConfig)

		var awsGlueJobNameList []string
		awsGlueJobNameList, err = getGlueJobNameList(ctx, awsGlueClient)
		if err != nil {
			log.Printf("%v", err)
		}

		awsGlueJobCustomMetricsPublishingResult, err := calculateAndPublishAwsGlueJobCustomMetricValues(ctx, awsGlueClient, awsCloudWatchClient, awsGlueJobNameList)
		awsGluejobNameAndEligibleJobRunsMap, err := findTheEligibleCurrentAwsGlueJobRunsToMonitor(ctx, awsGlueClient, awsGlueJobNameList)
		awsGlueJobRunCustomMetricsPublishResult, err := prepareTheListOfEachAwsGlueJobRunCustomMetricsAndPublishSequentially(ctx, awsGlueClient, awsCloudWatchClient, awsGluejobNameAndEligibleJobRunsMap)

		if err != nil {
			log.Printf("%v", err)
		} else {
			log.Printf("%s", awsGlueJobCustomMetricsPublishingResult, awsGlueJobRunCustomMetricsPublishResult)
		}
	}
}

func getGlueJobNameList(ctx context.Context, awsGlueClient *glue.Client) ([]string, error) {
	var awsGlueJobNameList []string
	input := &glue.ListJobsInput{}
	for {
		glueJobList, err := awsGlueClient.ListJobs(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the list of glue jobs: %v", err)
		}
		awsGlueJobNameList = append(awsGlueJobNameList, glueJobList.JobNames...)
		if glueJobList.NextToken == nil {
			break
		}
		input.NextToken = glueJobList.NextToken
	}
	return awsGlueJobNameList, nil
}

func calculateAndPublishAwsGlueJobCustomMetricValues(ctx context.Context, awsGlueClient *glue.Client, awsCloudWatchClient *cloudwatch.Client,
	awsGlueJobNameList []string) (string, error) {
	for _, awsGlueJobName := range awsGlueJobNameList {
		var currentAwsGlueJobTotalRunCount int
		var awsGlueJobAverageExecutionDurationSec int32
		var awsGlueJobCustomMetricNameAndValueMap = make(map[string]float64)
		var awsGlueJobCustomMetricNameAndUnitMap = make(map[string]types.StandardUnit)
		awsGlueJobRunIdAll := "ALL"
		var awsGlueJobLatestRunErrorMessage *string

		currentGlueJobAllRuns, err := awsGlueClient.GetJobRuns(ctx, &glue.GetJobRunsInput{JobName: aws.String(awsGlueJobName)})
		if err == nil && currentGlueJobAllRuns != nil {
			currentAwsGlueJobTotalRunCount = len(currentGlueJobAllRuns.JobRuns)
			awsGlueJobAverageExecutionDurationSec = computeAwsGlueJobRunsAverageExecutionDuration(currentGlueJobAllRuns, currentAwsGlueJobTotalRunCount)

			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME] = float64(currentAwsGlueJobTotalRunCount)
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME] = types.StandardUnitCount

			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME] = float64(awsGlueJobAverageExecutionDurationSec)
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds

			awsGlueJobLatestRunDetails := currentGlueJobAllRuns.JobRuns[0]
			awsGlueJobLatestRunStateCode := computeAWSGlueJobLatestRunStateMetric(awsGlueJobLatestRunDetails.JobRunState)
			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME] = float64(awsGlueJobLatestRunStateCode)
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME] = types.StandardUnitNone

			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME] = float64(currentGlueJobAllRuns.JobRuns[0].ExecutionTime)
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds

			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME] = float64(currentGlueJobAllRuns.JobRuns[0].StartedOn.Unix())
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds

			awsGlueJobRunCompletedOn := currentGlueJobAllRuns.JobRuns[0].CompletedOn
			if awsGlueJobRunCompletedOn != nil {
				awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME] = float64(awsGlueJobRunCompletedOn.Unix())
				awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds
			}

			awsGlueJobLatestRunErrorMessage = currentGlueJobAllRuns.JobRuns[0].ErrorMessage
			hasErrorOccurred := getAWSGlueJobErrorStateCode(awsGlueJobLatestRunErrorMessage)
			awsGlueJobCustomMetricNameAndValueMap[AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME] = hasErrorOccurred
			awsGlueJobCustomMetricNameAndUnitMap[AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME] = types.StandardUnitNone
		}
		err = constructGlueCustomMetricMetadataAndPublish(ctx, awsGlueJobName,
			awsCloudWatchClient, awsGlueJobCustomMetricNameAndValueMap, awsGlueJobCustomMetricNameAndUnitMap, awsGlueJobRunIdAll, awsGlueJobLatestRunErrorMessage)

		if err != nil {
			return "Failed to Publish Glue Job Custom Metrics for: " + awsGlueJobName, err
		}
	}
	return "Glue Job Custom Metrics Published", nil
}

func findTheEligibleCurrentAwsGlueJobRunsToMonitor(ctx context.Context, awsGlueClient *glue.Client,
	awsGlueJobNameList []string) (map[string][]types2.JobRun, error) {
	var awsGluejobNameAndEligibleJobRunsMap = make(map[string][]types2.JobRun)
	var errorMessage error
	for _, awsGlueJobName := range awsGlueJobNameList {
		var awsGlueJobRunToBeMonitored []types2.JobRun
		currentGlueJobAllRuns, err := awsGlueClient.GetJobRuns(ctx, &glue.GetJobRunsInput{JobName: aws.String(awsGlueJobName)})
		errorMessage = err
		if errorMessage == nil && currentGlueJobAllRuns != nil {
			currentTime := time.Now()
			nineHundredSecondsAgoTime := currentTime.Add(-900 * time.Second)
			for _, currentAwsGlueJobRun := range currentGlueJobAllRuns.JobRuns {
				if (currentAwsGlueJobRun.CompletedOn == nil && isTheAWSGlueJobRunActive(currentAwsGlueJobRun.JobRunState)) || (currentAwsGlueJobRun.
					CompletedOn != nil && currentAwsGlueJobRun.CompletedOn.Compare(nineHundredSecondsAgoTime) >= 0) {
					awsGlueJobRunToBeMonitored = append(awsGlueJobRunToBeMonitored, currentAwsGlueJobRun)
				}
				awsGluejobNameAndEligibleJobRunsMap[awsGlueJobName] = awsGlueJobRunToBeMonitored
			}
		}
	}
	return awsGluejobNameAndEligibleJobRunsMap, errorMessage
}

func prepareTheListOfEachAwsGlueJobRunCustomMetricsAndPublishSequentially(ctx context.Context, awsGlueClient *glue.Client, awsCloudWatchClient *cloudwatch.Client,
	awsGlueJobNameAndEligibleJobRunsMap map[string][]types2.JobRun) (string, error) {

	if awsGlueJobNameAndEligibleJobRunsMap != nil {
		for awsGlueJobName, awsGlueJobRuns := range awsGlueJobNameAndEligibleJobRunsMap {
			if len(awsGlueJobRuns) > 0 {
				for _, awsGlueJobRun := range awsGlueJobRuns {
					var awsGlueJobRunCustomMetricNameAndValueMap = make(map[string]float64)
					var awsGlueJobRunCustomMetricNameAndUnitMap = make(map[string]types.StandardUnit)
					var awsGlueJobRunErrorMessage *string

					awsGlueJobRunStateCode := computeAWSGlueJobLatestRunStateMetric(awsGlueJobRun.JobRunState)
					awsGlueJobRunCustomMetricNameAndValueMap[AWS_GLUE_JOB_RUN_CURRENT_STATE_CUSTOM_METRIC_NAME] = float64(awsGlueJobRunStateCode)
					awsGlueJobRunCustomMetricNameAndUnitMap[AWS_GLUE_JOB_RUN_CURRENT_STATE_CUSTOM_METRIC_NAME] = types.StandardUnitNone

					awsGlueJobRunCustomMetricNameAndValueMap[AWS_GLUE_JOB_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME] = float64(awsGlueJobRun.StartedOn.Unix())
					awsGlueJobRunCustomMetricNameAndUnitMap[AWS_GLUE_JOB_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds

					awsGlueJobRunCompletedOn := awsGlueJobRun.CompletedOn
					if awsGlueJobRunCompletedOn != nil {
						awsGlueJobRunCustomMetricNameAndValueMap[AWS_GLUE_JOB_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME] = float64(awsGlueJobRun.CompletedOn.Unix())
						awsGlueJobRunCustomMetricNameAndUnitMap[AWS_GLUE_JOB_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds
					}

					awsGlueJobRunCustomMetricNameAndValueMap[AWS_GLUE_JOB_RUN_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME] = float64(awsGlueJobRun.ExecutionTime)
					awsGlueJobRunCustomMetricNameAndUnitMap[AWS_GLUE_JOB_RUN_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME] = types.StandardUnitSeconds

					awsGlueJobRunErrorMessage = awsGlueJobRun.ErrorMessage
					hasErrorOccurred := getAWSGlueJobErrorStateCode(awsGlueJobRunErrorMessage)
					awsGlueJobRunCustomMetricNameAndValueMap[AWS_GLUE_JOB_RUN_ERROR_STATE_CUSTOM_METRIC_NAME] = float64(hasErrorOccurred)
					awsGlueJobRunCustomMetricNameAndUnitMap[AWS_GLUE_JOB_RUN_ERROR_STATE_CUSTOM_METRIC_NAME] = types.StandardUnitNone

					err := constructGlueCustomMetricMetadataAndPublish(ctx, awsGlueJobName,
						awsCloudWatchClient, awsGlueJobRunCustomMetricNameAndValueMap, awsGlueJobRunCustomMetricNameAndUnitMap, *awsGlueJobRun.Id, awsGlueJobRunErrorMessage)

					if err != nil {
						return "Failed to Publish Glue Job Run Custom Metrics for Job: " + awsGlueJobName + "and Run Id: " + *awsGlueJobRun.Id, err
					}
				}
			}
		}
	}
	return "Glue Job Run Custom Metrics Published", nil
}

func computeAwsGlueJobRunsAverageExecutionDuration(currentGlueJobAllRuns *glue.GetJobRunsOutput, currentGlueJobTotalRunCount int) int32 {
	var sumOfExecutionDurationOfAllJobRuns int32
	var awsGlueJobAverageExecutionDurationSec int32
	for _, jobRunDetails := range currentGlueJobAllRuns.JobRuns {
		sumOfExecutionDurationOfAllJobRuns += jobRunDetails.ExecutionTime
	}
	awsGlueJobAverageExecutionDurationSec = (sumOfExecutionDurationOfAllJobRuns) / (int32(currentGlueJobTotalRunCount))
	return awsGlueJobAverageExecutionDurationSec
}

func computeAWSGlueJobLatestRunStateMetric(awsGlueJobRunState types2.JobRunState) int {

	var awsGlueJobLatestRunStateCode int

	switch awsGlueJobRunState {
	case "STARTING":
		awsGlueJobLatestRunStateCode = 0
	case "RUNNING":
		awsGlueJobLatestRunStateCode = 1
	case "STOPPING":
		awsGlueJobLatestRunStateCode = 2
	case "STOPPED":
		awsGlueJobLatestRunStateCode = 3
	case "SUCCEEDED":
		awsGlueJobLatestRunStateCode = 4
	case "FAILED":
		awsGlueJobLatestRunStateCode = 5
	case "TIMEOUT":
		awsGlueJobLatestRunStateCode = 6
	case "ERROR":
		awsGlueJobLatestRunStateCode = 7
	case "WAITING":
		awsGlueJobLatestRunStateCode = 8
	case "EXPIRED":
		awsGlueJobLatestRunStateCode = 9
	default:
		awsGlueJobLatestRunStateCode = -1
	}

	return awsGlueJobLatestRunStateCode
}

func isTheAWSGlueJobRunActive(awsGlueJobRunState types2.JobRunState) bool {
	var isActive bool
	awsGlueJobLatestRunStateCode := computeAWSGlueJobLatestRunStateMetric(awsGlueJobRunState)

	switch awsGlueJobLatestRunStateCode {
	case 0:
		isActive = true
	case 1:
		isActive = true
	case 2:
		isActive = true
	case 8:
		isActive = true
	default:
		isActive = false

	}
	return isActive
}

func getAWSGlueJobErrorStateCode(awsGlueJobLatestRunErrorMessage *string) float64 {
	var hasErrorOccurred float64
	if awsGlueJobLatestRunErrorMessage != nil {
		hasErrorOccurred = 1.0
	} else {
		hasErrorOccurred = 0.0
	}
	return hasErrorOccurred
}

func constructGlueCustomMetricMetadataAndPublish(ctx context.Context, awsGlueJobName string, awsCloudWatchClient *cloudwatch.Client,
	awsGlueCustomMetricNameAndValueMap map[string]float64, awsGlueCustomMetricNameAndUnitMap map[string]types.StandardUnit,
	awsGlueJobRunId string, awsGlueJobRunErrorMessage *string) error {

	noErrorMsg := "No error"
	var awsGlueJobCustomMetricMetaData []types.MetricDatum
	var awsGlueJobCustomMetricNamespace = "Broadcom/AwsCustomMetric"
	if awsGlueJobRunErrorMessage == nil {
		awsGlueJobRunErrorMessage = &noErrorMsg
	}
	for metricName, metricValue := range awsGlueCustomMetricNameAndValueMap {
		awsGlueJobCustomMetricMetaData = append(awsGlueJobCustomMetricMetaData, types.MetricDatum{
			MetricName: aws.String(metricName),
			Value:      aws.Float64(metricValue),
			Unit:       awsGlueCustomMetricNameAndUnitMap[metricName],
			Timestamp:  aws.Time(time.Now()),
			Dimensions: []types.Dimension{
				{Name: aws.String("JobName"), Value: aws.String(awsGlueJobName)},
				{Name: aws.String("JobRunId"), Value: aws.String(awsGlueJobRunId)},
				{Name: aws.String("Error Message"), Value: aws.String(*awsGlueJobRunErrorMessage)},
				{Name: aws.String("namespace"), Value: aws.String("Glue")},
			},
		})

		/*fmt.Println("Details of the metrics getting published:")
		fmt.Println("AWS Glue Job Name: ", awsGlueJobName)
		fmt.Println("AWS Glue Job Run Id: ", awsGlueJobRunId)
		fmt.Println("Metric Name: ", metricName)
		fmt.Println("Metric Value: ", metricValue)
		fmt.Println("Metric Unit: ", awsGlueCustomMetricNameAndUnitMap[metricName])
		fmt.Println("Error Message: ", aws.String(*awsGlueJobRunErrorMessage))
		fmt.Println("Namespace: ", aws.String("Glue"))
		fmt.Println("---------------------------------------------")*/
	}

	awsGlueJobCustomMetricMetaDataInput := &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(awsGlueJobCustomMetricNamespace),
		MetricData: awsGlueJobCustomMetricMetaData,
	}

	_, err := awsCloudWatchClient.PutMetricData(ctx, awsGlueJobCustomMetricMetaDataInput)
	if err != nil {
		return fmt.Errorf("failed to push custom metrics to CloudWatch for job '%s': %v", awsGlueJobName, err)
	}
	log.Printf("Successfully pushed metrics for job '%s' to CloudWatch\n", awsGlueJobName)

	return nil
}
