package com.broadcom.ca.apm.cloudmonitoring;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.JobRun;

import java.util.List;
import java.util.Map;

public class GlueCustomMetricsLambda {

    private static final String NAMESPACE = "Broadcom/AwsCustomMetric";

    public void handleRequest(Map<String, Object> event) {
        GlueClient glueClient = GlueClient.create();
        CloudWatchClient cloudWatchClient = CloudWatchClient.create();

        List<String> glueJobNames = GlueCustomMetricsPublisher.getGlueJobNameList(glueClient);
        glueJobNames.forEach(jobName -> {
            List<JobRun> jobRuns = GlueCustomMetricsPublisher.getGlueJobRuns(glueClient, jobName);
            if (!jobRuns.isEmpty()) {
                GlueCustomMetricsPublisher.publishJobMetrics(cloudWatchClient, jobName, jobRuns);
            }
        });
    }
}
