package com.datasourcefetch.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.datasourcefetch.model.QueryTask;

@Configuration
@EnableBatchProcessing
public class DynamicJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobRepository jobRepository;
    
    private ScheduledFuture<?> scheduledFuture;
    
    @Autowired
    @Qualifier("mysqlJdbcTemplate")
    private static JdbcTemplate mysqlJdbcTemplate;
    
    @Autowired
    private TaskScheduler taskScheduler;
    
    public DynamicJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, JobRepository jobRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobRepository = jobRepository;
    }

    @Bean
    public Job createJob(String jobName, QueryTask task) {
        JobBuilder jobBuilder = jobBuilderFactory.get(jobName);
        Step step = createStep(jobName+"Step",task);
        return jobBuilder.start(step).build();
     }

    private Step createStep(String stepName,QueryTask task) {
        return stepBuilderFactory.get(stepName)
                .<String, String>chunk(10)
                .reader(reader(task))
                .writer(writer())
                .build();
    }

    private ItemReader<String> reader(QueryTask task) {
    	
    	List<QueryTask> queryTask = runScheduler(task);
    	
    	System.out.println(queryTask.size());
    	
        return () -> "Sample data";
    }
    private List<QueryTask> runScheduler(QueryTask task){
    	List<QueryTask> processedTasks = new ArrayList<>();
    	
    	// Schedule the job using the cron expression
        if (task.getScheduler() != null) {
            scheduledFuture = taskScheduler.schedule(() -> {
                try {
                	int threadCount = task.getThread(); // replace with input count
                    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

                    for (int i = 0; i < threadCount; i++) {
                        executorService.submit(new RecordProcessor(i + 1,task));
                    }

                    executorService.shutdown();
                    List<QueryTask> processedTasksFromTable = getRecordsFromTable(task);
                    processedTasks.addAll(processedTasksFromTable);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, new CronTrigger(task.getScheduler()));
        }
        
        return processedTasks;
    }
    
   public static List<QueryTask> getRecordsFromTable(QueryTask task){
    	List<QueryTask> processedTasks = new ArrayList<>();
    	List<String> results = null;
		try {
			results = executeQueryOnMysql(task.getQuery());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        for (int i = 0; i < results.size(); i += task.getChunkSize()) {
            int end = Math.min(i + task.getChunkSize(), results.size());
            List<String> chunk = results.subList(i, end);
            
            QueryTask processedTask = new QueryTask();
            processedTask.setId(task.getId());
            processedTask.setQuery(task.getQuery());
            processedTask.setChunkSize(task.getChunkSize());
            processedTask.setStatus("COMPLETED");
            processedTask.setResult(String.join("\n", chunk));
            
            processedTasks.add(processedTask);
            System.out.println("Chunk " + (i/task.getChunkSize() + 1) + ": " + processedTask.getResult());
        }
        
        return processedTasks;
    }
    
    private static List<String> executeQueryOnMysql(String query) {
        return mysqlJdbcTemplate.queryForList(query, String.class);
    }

    private ItemWriter<String> writer() {
        return items -> items.forEach(System.out::println);
    }
}