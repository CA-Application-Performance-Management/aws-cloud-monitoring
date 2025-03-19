package com.broadcom.ca.apm.cloudmonitoring;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlueMetricsPublisherLambda
{

    private static final String AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME = "Execution Count";
    private static final String AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME = "Average Execution Duration (Sec)";
    private static final String AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME = "Latest Run State";
    private static final String AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME = "Latest Run Execution Duration (Sec)";
    private static final String AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME = "Latest Run Start Time (Epoch)";
    private static final String AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME = "Latest Run Completion Time (Epoch)";
    private static final String AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME = "Latest Run Error State";

    public static void main(String[] args) {
        try {
            lambdaHandler();
        }
        catch (Exception e) {
            System.err.println("Handler Failed: " + e.getMessage());
        }
    }

    public static void lambdaHandler() throws Exception {
        GlueClient glueClient = GlueClient.builder()
                                          .region(Region.AP_SOUTH_1)
                                          .credentialsProvider(DefaultCredentialsProvider.create())
                                          .build();

        CloudWatchClient cloudWatchClient = CloudWatchClient.builder()
                                                            .region(Region.AP_SOUTH_1)
                                                            .credentialsProvider(DefaultCredentialsProvider.create())
                                                            .build();

        List<String> glueJobNames = getGlueJobNameList(glueClient);
        calculateAndPublishAwsGlueJobCustomMetricValues(glueClient, cloudWatchClient, glueJobNames);
        Map<String, List<JobRun>> glueJobNameAndEligibleJobRunsMap = findTheEligibleCurrentAwsGlueJobRunsToMonitor(glueClient, glueJobNames);
        String awsGlueJobRunCustomMetricPublishingOutput = prepareTheListOfEachAwsGlueJobRunCustomMetricsAndPublishSequentially(glueClient,
                                                                                                                                cloudWatchClient,
                                                                                                                                glueJobNameAndEligibleJobRunsMap);
        System.out.println(awsGlueJobRunCustomMetricPublishingOutput);
    }

    public static List<String> getGlueJobNameList(GlueClient glueClient) {
        ListJobsRequest request = ListJobsRequest.builder().build();
        ListJobsResponse response = glueClient.listJobs(request);
        return response.jobNames();
    }

    public static void calculateAndPublishAwsGlueJobCustomMetricValues(GlueClient glueClient, CloudWatchClient cloudWatchClient,
                                                                       List<String> glueJobNames) throws Exception
    {
        for (String jobName : glueJobNames) {

            List<JobRun> currentGlueJobAllRuns = fetchAllJobRunsOfGlueJob(glueClient, jobName);
            int totalRunCount = currentGlueJobAllRuns.size();
            System.out.println("totalRunCount: " + totalRunCount);
            int averageExecutionDurationSec = computeAwsGlueJobRunsAverageExecutionDuration(currentGlueJobAllRuns, totalRunCount);

            Map<String, Double> metricNameAndValueMap = new HashMap<>();
            Map<String, StandardUnit> metricNameAndUnitMap = new HashMap<>();

            metricNameAndValueMap.put(AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME, (double) totalRunCount);
            metricNameAndUnitMap.put(AWS_GLUE_JOB_EXECUTION_COUNT_CUSTOM_METRIC_NAME, StandardUnit.COUNT);

            metricNameAndValueMap.put(AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME, (double) averageExecutionDurationSec);
            metricNameAndUnitMap.put(AWS_GLUE_JOB_AVG_EXECUTION_DURATION_SEC_CUSTOM_METRIC_NAME, StandardUnit.SECONDS);

            JobRun latestRun = currentGlueJobAllRuns.get(0);
            metricNameAndValueMap.put(AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME,
                                      (double) computeAWSGlueJobLatestRunStateMetric(String.valueOf(latestRun.jobRunState())));
            metricNameAndUnitMap.put(AWS_GLUE_JOB_LATEST_RUN_STATE_CUSTOM_METRIC_NAME, StandardUnit.NONE);

            metricNameAndValueMap.put(AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME, (double) latestRun.executionTime());
            metricNameAndUnitMap.put(AWS_GLUE_JOB_LATEST_RUN_EXEC_DURATION_SEC_CUSTOM_METRIC_NAME, StandardUnit.SECONDS);

            metricNameAndValueMap.put(AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME, (double) latestRun.startedOn().getEpochSecond());
            metricNameAndUnitMap.put(AWS_GLUE_JOB_LATEST_RUN_START_TIME_EPOCH_CUSTOM_METRIC_NAME, StandardUnit.SECONDS);

            if (latestRun.completedOn() != null) {
                metricNameAndValueMap.put(AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME,
                                          (double) latestRun.completedOn().getEpochSecond());
                metricNameAndUnitMap.put(AWS_GLUE_JOB_LATEST_RUN_COMPLETION_TIME_EPOCH_CUSTOM_METRIC_NAME, StandardUnit.SECONDS);
            }

            String latestRunErrorMessage = latestRun.errorMessage();
            double errorState = getAWSGlueJobErrorStateCode(latestRunErrorMessage);
            metricNameAndValueMap.put(AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME, errorState);
            metricNameAndUnitMap.put(AWS_GLUE_JOB_LATEST_RUN_ERROR_STATE_CUSTOM_METRIC_NAME, StandardUnit.NONE);

            constructGlueCustomMetricMetadataAndPublish(jobName, cloudWatchClient, metricNameAndValueMap, metricNameAndUnitMap, "ALL",
                                                        latestRunErrorMessage);
        }
    }

    public static List<JobRun> fetchAllJobRunsOfGlueJob(GlueClient glueClient, String jobName) {
        List<JobRun> glueJobAllRuns = new ArrayList<>();
        String nextToken = null;

        do {
            GetJobRunsRequest request = GetJobRunsRequest.builder()
                                                         .jobName(jobName)
                                                         .nextToken(nextToken)
                                                         .build();

            GetJobRunsResponse response = glueClient.getJobRuns(request);
            glueJobAllRuns.addAll(response.jobRuns());
            nextToken = response.nextToken();
        }
        while (nextToken != null);

        return glueJobAllRuns;
    }

    public static Map<String, List<JobRun>> findTheEligibleCurrentAwsGlueJobRunsToMonitor(GlueClient glueClient, List<String> glueJobNames) {
        Map<String, List<JobRun>> glueJobNameAndEligibleJobRunsMap = new HashMap<>();
        for (String glueJobName : glueJobNames) {
            List<JobRun> currentGlueJobAllRuns = fetchAllJobRunsOfGlueJob(glueClient, glueJobName);;

            List<JobRun> eligibleJobRuns = new ArrayList<>();
            for (JobRun jobRun : currentGlueJobAllRuns) {
                if ((jobRun.completedOn() == null && isTheAWSGlueJobRunActive(jobRun.jobRunStateAsString())) ||
                    (jobRun.completedOn() != null && jobRun.completedOn().isAfter(Instant.now().minusSeconds(900))))
                {
                    eligibleJobRuns.add(jobRun);
                }
            }

            glueJobNameAndEligibleJobRunsMap.put(glueJobName, eligibleJobRuns);
        }
        return glueJobNameAndEligibleJobRunsMap;
    }

    public static String prepareTheListOfEachAwsGlueJobRunCustomMetricsAndPublishSequentially(GlueClient glueClient,
                                                                                              CloudWatchClient cloudWatchClient,
                                                                                              Map<String, List<JobRun>> glueJobNameAndEligibleJobRunsMap)
    {
        if (glueJobNameAndEligibleJobRunsMap != null) {
            for (Map.Entry<String, List<JobRun>> entry : glueJobNameAndEligibleJobRunsMap.entrySet()) {
                String glueJobName = entry.getKey();
                List<JobRun> glueJobRuns = entry.getValue();

                if (!glueJobRuns.isEmpty()) {
                    for (JobRun glueJobRun : glueJobRuns) {
                        Map<String, Double> glueJobRunCustomMetricNameAndValueMap = new HashMap<>();
                        Map<String, StandardUnit> glueJobRunCustomMetricNameAndUnitMap = new HashMap<>();
                        String glueJobRunErrorMessage = glueJobRun.errorMessage();

                        int glueJobRunStateCode = computeAWSGlueJobLatestRunStateMetric(String.valueOf(glueJobRun.jobRunState()));
                        glueJobRunCustomMetricNameAndValueMap.put("Current Run State", (double) glueJobRunStateCode);
                        glueJobRunCustomMetricNameAndUnitMap.put("Current Run State", StandardUnit.NONE);

                        glueJobRunCustomMetricNameAndValueMap.put("Run Start Time (Epoch)", (double) glueJobRun.startedOn().getEpochSecond());
                        glueJobRunCustomMetricNameAndUnitMap.put("Run Start Time (Epoch)", StandardUnit.SECONDS);

                        if (glueJobRun.completedOn() != null) {
                            glueJobRunCustomMetricNameAndValueMap.put("Run Completion Time (Epoch)",
                                                                      (double) glueJobRun.completedOn().getEpochSecond());
                            glueJobRunCustomMetricNameAndUnitMap.put("Run Completion Time (Epoch)", StandardUnit.SECONDS);
                        }

                        glueJobRunCustomMetricNameAndValueMap.put("Run Execution Duration (Sec)", (double) glueJobRun.executionTime());
                        glueJobRunCustomMetricNameAndUnitMap.put("Run Execution Duration (Sec)", StandardUnit.SECONDS);

                        double hasErrorOccurred = getAWSGlueJobErrorStateCode(glueJobRunErrorMessage);
                        glueJobRunCustomMetricNameAndValueMap.put("Run Error State", hasErrorOccurred);
                        glueJobRunCustomMetricNameAndUnitMap.put("Run Error State", StandardUnit.NONE);

                        try {
                            constructGlueCustomMetricMetadataAndPublish(glueJobName, cloudWatchClient, glueJobRunCustomMetricNameAndValueMap,
                                                                        glueJobRunCustomMetricNameAndUnitMap, glueJobRun.id(),
                                                                        glueJobRunErrorMessage);
                        }
                        catch (Exception e) {
                            return "Failed to Publish Glue Job Run Custom Metrics for Job: " + glueJobName + " and Run Id: " + glueJobRun.id();
                        }
                    }
                }
            }
        }
        return "Glue Job Run Custom Metrics Published";
    }

    public static int computeAwsGlueJobRunsAverageExecutionDuration(List<JobRun> jobRuns, int totalRunCount) {
        int sumOfExecutionDurationOfAllJobRuns = 0;
        for (JobRun jobRunDetails : jobRuns) {
            sumOfExecutionDurationOfAllJobRuns += jobRunDetails.executionTime();
        }
        return sumOfExecutionDurationOfAllJobRuns / totalRunCount;
    }

    public static int computeAWSGlueJobLatestRunStateMetric(String jobRunState) {
        int awsGlueJobLatestRunStateCode;
        switch (jobRunState) {
            case "STARTING":
                awsGlueJobLatestRunStateCode = 0;
                break;
            case "RUNNING":
                awsGlueJobLatestRunStateCode = 1;
                break;
            case "STOPPING":
                awsGlueJobLatestRunStateCode = 2;
                break;
            case "STOPPED":
                awsGlueJobLatestRunStateCode = 3;
                break;
            case "SUCCEEDED":
                awsGlueJobLatestRunStateCode = 4;
                break;
            case "FAILED":
                awsGlueJobLatestRunStateCode = 5;
                break;
            case "TIMEOUT":
                awsGlueJobLatestRunStateCode = 6;
                break;
            case "ERROR":
                awsGlueJobLatestRunStateCode = 7;
                break;
            case "WAITING":
                awsGlueJobLatestRunStateCode = 8;
                break;
            case "EXPIRED":
                awsGlueJobLatestRunStateCode = 9;
                break;
            default:
                awsGlueJobLatestRunStateCode = -1;
                break;
        }
        return awsGlueJobLatestRunStateCode;
    }

    public static boolean isTheAWSGlueJobRunActive(String jobRunState) {
        int awsGlueJobLatestRunStateCode = computeAWSGlueJobLatestRunStateMetric(jobRunState);
        return awsGlueJobLatestRunStateCode == 0 || awsGlueJobLatestRunStateCode == 1 || awsGlueJobLatestRunStateCode == 2 ||
               awsGlueJobLatestRunStateCode == 8;
    }

    public static double getAWSGlueJobErrorStateCode(String awsGlueJobLatestRunErrorMessage) {
        return awsGlueJobLatestRunErrorMessage != null ? 1.0 : 0.0;
    }

    public static void constructGlueCustomMetricMetadataAndPublish(String awsGlueJobName, CloudWatchClient awsCloudWatchClient,
                                                                   Map<String, Double> awsGlueCustomMetricNameAndValueMap,
                                                                   Map<String, StandardUnit> awsGlueCustomMetricNameAndUnitMap,
                                                                   String awsGlueJobRunId, String awsGlueJobRunErrorMessage) throws Exception
    {

        String noErrorMsg = "No Error";
        List<MetricDatum> awsGlueJobCustomMetricMetaData = new ArrayList<>();
        String awsGlueJobCustomMetricNamespace = "Broadcom/AwsCustomMetric";

        if (awsGlueJobRunErrorMessage == null) {
            awsGlueJobRunErrorMessage = noErrorMsg;
        }

        for (Map.Entry<String, Double> entry : awsGlueCustomMetricNameAndValueMap.entrySet()) {
            String metricName = entry.getKey();
            Double metricValue = entry.getValue();

            MetricDatum metricDatum = MetricDatum.builder()
                                                 .metricName(metricName)
                                                 .value(metricValue)
                                                 .unit(awsGlueCustomMetricNameAndUnitMap.get(metricName))
                                                 .timestamp(Instant.now())
                                                 .dimensions(
                                                         Dimension.builder().name("JobName").value(awsGlueJobName).build(),
                                                         Dimension.builder().name("JobRunId").value(awsGlueJobRunId).build(),
                                                         Dimension.builder().name("ErrorMessage").value(awsGlueJobRunErrorMessage).build(),
                                                         Dimension.builder().name("Namespace").value("Glue").build()
                                                            )
                                                 .build();

            awsGlueJobCustomMetricMetaData.add(metricDatum);

            System.out.println("Details of the metrics getting published:");
            System.out.println("AWS Glue Job Name: " + awsGlueJobName);
            System.out.println("AWS Glue Job Run Id: " + awsGlueJobRunId);
            System.out.println("Metric Name: " + metricName);
            System.out.println("Metric Value: " + metricValue);
            System.out.println("Metric Unit: " + awsGlueCustomMetricNameAndUnitMap.get(metricName));
            System.out.println("Error Message: " + awsGlueJobRunErrorMessage);
            System.out.println("Namespace: Glue");
            System.out.println("---------------------------------------------");
        }

        PutMetricDataRequest request = PutMetricDataRequest.builder()
                                                           .namespace(awsGlueJobCustomMetricNamespace)
                                                           .metricData(awsGlueJobCustomMetricMetaData)
                                                           .build();

        PutMetricDataResponse putMetricDataResponseFromCloudWatch = awsCloudWatchClient.putMetricData(request);
        System.out.println("PutMetricData Operation is Successful: " + putMetricDataResponseFromCloudWatch.sdkHttpResponse().isSuccessful());
        System.out.println("Successfully pushed metrics for job " + awsGlueJobName + " to CloudWatch");
    }
}
