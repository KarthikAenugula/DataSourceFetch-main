/**
 * 
 */
package com.datasourcefetch.config;

import java.sql.ResultSet;
import java.util.List;

import com.datasourcefetch.model.QueryTask;

/**
 * 
 */
public class RecordProcessor implements Runnable {

    private int threadNumber;
    private QueryTask task;

    public RecordProcessor(int threadNumber, QueryTask task) {
        this.threadNumber = threadNumber;
        this.task=task;
    }

    @Override
    public void run() {
        System.out.println("Thread " + threadNumber + " is starting...");

        try  {
        	List<QueryTask> resultSet = DynamicJobConfig.getRecordsFromTable(task);
            while (resultSet != null ) {
                QueryTask recordData = resultSet.get(0); // Replace with your column name
                System.out.println("Thread " + threadNumber + " processed record: " + recordData);
                Thread.sleep(500); // Simulate processing time
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
