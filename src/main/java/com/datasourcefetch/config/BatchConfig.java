package com.datasourcefetch.config;

import com.datasourcefetch.model.QueryTask;
import com.datasourcefetch.processor.QueryTaskProcessor;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Bean
    public Job queryExecutionJob(JobBuilderFactory jobBuilderFactory,
                                 Step queryExecutionStep) {
        return jobBuilderFactory.get("queryExecutionJob")
                .start(queryExecutionStep)
                .build();
    }

    @Bean
    public Step queryExecutionStep(StepBuilderFactory stepBuilderFactory,
                                   ItemReader<QueryTask> reader,
                                   QueryTaskProcessor processor,
                                   ItemWriter<QueryTask> writer) {
        return stepBuilderFactory.get("queryExecutionStep")
                .<QueryTask, List<QueryTask>>chunk(1)
                .reader(reader)
                .processor(processor)
                .writer(new CompositeItemWriter<>(writer))
                .build();
    }

    public class CompositeItemWriter<T> implements ItemWriter<List<T>> {
        private final ItemWriter<T> delegate;

        public CompositeItemWriter(ItemWriter<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(List<? extends List<T>> items) throws Exception {
            for (List<T> item : items) {
                delegate.write(item);
            }
        }
    }
}