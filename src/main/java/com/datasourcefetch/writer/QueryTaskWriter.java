package com.datasourcefetch.writer;

import com.datasourcefetch.model.QueryTask;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class QueryTaskWriter implements ItemWriter<QueryTask> {

    @Autowired
    @Qualifier("postgresJdbcTemplate")
    private JdbcTemplate postgresJdbcTemplate;

    @Override
    public void write(List<? extends QueryTask> items) throws Exception {
        for (QueryTask task : items) {
            updateTaskStatus(task);
        }
    }

    private void updateTaskStatus(QueryTask task) {
        String sql = "UPDATE postgre_table SET status = ? WHERE id = ?";
        postgresJdbcTemplate.update(sql, task.getStatus(), task.getId());
    }
}