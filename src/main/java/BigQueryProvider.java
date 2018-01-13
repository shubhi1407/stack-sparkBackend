import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BigQueryProvider {
  public static ArrayList<TagsObj> fetchRecords(String Query) throws Exception {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(Query)
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    QueryResponse response = bigquery.getQueryResults(jobId);

    QueryResult result = response.getResult();
     ArrayList<TagsObj> tags = new ArrayList<>();
    // Print all pages of the results.
    while (result != null ) {
      for (List<FieldValue> row : result.iterateAll()) {
    	 String[] eachTag = row.get(0).getStringValue().split("\\|");  
        for(int i=0;i<eachTag.length;i++) {
        	tags.add(new TagsObj(eachTag[i]));
        }  
        }
      result=result.getNextPage();
    }
   
    return tags;
  }
}
        