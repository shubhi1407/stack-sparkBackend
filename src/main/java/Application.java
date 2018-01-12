import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
		
	public static void main(String[] args) {
		String sourceTable = null;
		String targetTable = null;
		String classToRun = null;
		if(args.length==3) {
		sourceTable =args[0];
		 targetTable=args[1];
		 classToRun=args[2];
		}
		SparkSession sparkSession = SparkSession.builder().master("local").appName("stack-exchange-analysis")
				                    .getOrCreate();
		
        if(classToRun!=null && classToRun.equals("ProcessQueAndAns")) {
		ProcessQueAndAns countQueAndAns = new ProcessQueAndAns();
		countQueAndAns.countQueAndAns(sparkSession, sourceTable, targetTable);
		}
        else if(classToRun!=null && classToRun.equals("TagsCalc")) {
        	TagsCalc tagsCalc = new TagsCalc();
        	
        }else {

        	String Query = "SELECT tags FROM `bigquery-public-data.stackoverflow.posts_questions` limit 100 where tags!='' and tags is not null";
        	try {
				BigQueryProvider.fetchRecords(Query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	
}
}