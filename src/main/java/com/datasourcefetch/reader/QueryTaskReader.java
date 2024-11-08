package com.datasourcefetch.reader;

import com.datasourcefetch.model.QueryTask;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

@Component
public class QueryTaskReader implements ItemReader<QueryTask> {

    @Autowired
    @Qualifier("postgresJdbcTemplate")
    private JdbcTemplate postgresJdbcTemplate;

    

    @Override
    public QueryTask read() {
        Iterator<QueryTask> queryTaskIterator = null;
        List<QueryTask> queryTasks = fetchQueryTasksFromPostgres();
        System.out.println("QueryTaskReader: " + queryTasks.size());
        queryTaskIterator = queryTasks.iterator();
        return queryTaskIterator.hasNext() ? queryTaskIterator.next() : null;
    }

    private List<QueryTask> fetchQueryTasksFromPostgres() {
        String sql = "SELECT id, query, chunk_size, status FROM postgre_table WHERE status = 'PENDING'";
        System.out.println("QueryTaskReader: " + sql);
        return postgresJdbcTemplate.query(sql, (rs, rowNum) -> {
            QueryTask task = new QueryTask();
            task.setId(rs.getLong("id"));
            task.setQuery(rs.getString("query"));
            task.setChunkSize(rs.getInt("chunk_size"));
            task.setStatus(rs.getString("status"));
            return task;
        });
    }
}