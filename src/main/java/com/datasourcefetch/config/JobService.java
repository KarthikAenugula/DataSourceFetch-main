package com.datasourcefetch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import com.datasourcefetch.model.QueryTask;

@Service
public class JobService {

    private final JobLauncher jobLauncher;
    private final DynamicJobConfig jobConfig;

    public JobService(JobLauncher jobLauncher, DynamicJobConfig jobConfig) {
        this.jobLauncher = jobLauncher;
        this.jobConfig = jobConfig;
    }

    public void runJob(String jobName, QueryTask task) throws Exception {
        Job job = jobConfig.createJob(jobName,task);
        jobLauncher.run(job, new JobParametersBuilder().toJobParameters());
    }
}