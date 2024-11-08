package com.datasourcefetch.processor;

import com.datasourcefetch.config.JobService;
import com.datasourcefetch.model.QueryTask;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.ArrayList;

@Component
public class QueryTaskProcessor implements ItemProcessor<QueryTask, List<QueryTask>> {

   

    private ScheduledFuture<?> scheduledFuture;
    
    @Autowired
    private TaskScheduler taskScheduler;
    
    private JobService jobService;
    
    public QueryTaskProcessor(JobService jobService) {
        this.jobService = jobService;
    }
    
    @Override
    public List<QueryTask> process(QueryTask task) throws Exception {
    	
    	 createAndRunJob(task);
    	 
		return null;
    	
    	
    }

    private void createAndRunJob(QueryTask task) throws Exception {
    	
    	jobService.runJob(task.getJobName(), task);
    	
    	
    	
	}
  
    
}