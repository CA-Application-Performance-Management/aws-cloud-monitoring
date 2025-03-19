package com.broadcom.ca.apm.cloudmonitoring;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetJobRunsRequest;
import software.amazon.awssdk.services.glue.model.GetJobRunsResponse;
import software.amazon.awssdk.services.glue.model.JobRun;
import software.amazon.awssdk.services.glue.model.ListJobsRequest;
import software.amazon.awssdk.services.glue.model.ListJobsResponse;
import software.amazon.awssdk.services.sts.StsClient;

import java.time.Instant;
import java.util.*;

public class GlueCustomMetricsPublisher {

    private static final String NAMESPACE = "Broadcom/AwsCustomMetric";

    public static void main(String[] args) {
        GlueClient glueClient = GlueClient.create();
        CloudWatchClient cloudWatchClient = CloudWatchClient.create();

        List<String> glueJobNames = getGlueJobNameList(glueClient);
        glueJobNames.forEach(jobName -> {
            List<JobRun> jobRuns = getGlueJobRuns(glueClient, jobName);
            if (!jobRuns.isEmpty()) {
                publishJobMetrics(cloudWatchClient, jobName, jobRuns);
            }
        });
    }

    public static List<String> getGlueJobNameList(GlueClient glueClient) {
        List<String> jobNames = new ArrayList<>();
        String nextToken = null;
        do {
            ListJobsRequest request = ListJobsRequest.builder().nextToken(nextToken).build();
            ListJobsResponse response = glueClient.listJobs(request);
            jobNames.addAll(response.jobNames());
            nextToken = response.nextToken();
        } while (nextToken != null);
        return jobNames;
    }

    public static List<JobRun> getGlueJobRuns(GlueClient glueClient, String jobName) {
        GetJobRunsRequest request = GetJobRunsRequest.builder().jobName(jobName).build();
        GetJobRunsResponse response = glueClient.getJobRuns(request);
        return response.jobRuns();
    }

    public static void publishJobMetrics(CloudWatchClient cloudWatchClient, String jobName, List<JobRun> jobRuns) {
        int totalRuns = jobRuns.size();
        double avgExecutionDuration = jobRuns.stream().mapToInt(JobRun::executionTime).average().orElse(0);
        JobRun latestRun = jobRuns.get(0);

        List<MetricDatum> metricData = new ArrayList<>();

        metricData.add(createMetric("Execution Count", totalRuns, StandardUnit.COUNT, jobName, latestRun));
        metricData.add(createMetric("Average Execution Duration (Sec)", avgExecutionDuration, StandardUnit.SECONDS, jobName, latestRun));
        metricData.add(createMetric("Latest Run State", getStateCode(latestRun.jobRunStateAsString()), StandardUnit.NONE, jobName, latestRun));
        metricData.add(createMetric("Latest Run Execution Duration (Sec)", latestRun.executionTime(), StandardUnit.SECONDS, jobName, latestRun));
        metricData.add(createMetric("Latest Run Start Time (Epoch)", latestRun.startedOn().getEpochSecond(), StandardUnit.SECONDS, jobName, latestRun));

        if (latestRun.completedOn() != null) {
            metricData.add(createMetric("Latest Run Completion Time (Epoch)", latestRun.completedOn().getEpochSecond(), StandardUnit.SECONDS, jobName, latestRun));
        }

        double errorState = latestRun.errorMessage() != null ? 1.0 : 0.0;
        metricData.add(createMetric("Latest Run Error State", errorState, StandardUnit.NONE, jobName, latestRun));

        PutMetricDataRequest putMetricDataRequest = PutMetricDataRequest.builder()
                .namespace(NAMESPACE)
                .metricData(metricData)
                .build();

        cloudWatchClient.putMetricData(putMetricDataRequest);
        System.out.println("Successfully published metrics for job: " + jobName);
    }

    private static MetricDatum createMetric(String name, double value, StandardUnit unit, String jobName, JobRun run) {
        return MetricDatum.builder()
                .metricName(name)
                .value(value)
                .unit(unit)
                .timestamp(Instant.now())
                .dimensions(
                        Dimension.builder().name("JobName").value(jobName).build(),
                        Dimension.builder().name("JobRunId").value(run.id()).build(),
                        Dimension.builder().name("Error Message").value(run.errorMessage() != null ? run.errorMessage() : "No error").build(),
                        Dimension.builder().name("namespace").value("Glue").build()
                )
                .build();
    }

    private static int getStateCode(String state) {
        switch (state) {
            case "STARTING": return 0;
            case "RUNNING": return 1;
            case "STOPPING": return 2;
            case "STOPPED": return 3;
            case "SUCCEEDED": return 4;
            case "FAILED": return 5;
            case "TIMEOUT": return 6;
            case "ERROR": return 7;
            case "WAITING": return 8;
            case "EXPIRED": return 9;
            default: return -1;
        }
    }
}
