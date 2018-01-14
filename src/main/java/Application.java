import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.*;



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
		SparkSession sparkSession = SparkSession.builder().appName("stack-exchange-analysis")
				                    .getOrCreate();
		
        if(classToRun!=null && classToRun.equals("ProcessQueAndAns")) {
		ProcessQueAndAns countQueAndAns = new ProcessQueAndAns();
		countQueAndAns.countQueAndAns(sparkSession, sourceTable, targetTable);
		}
        else if(classToRun!=null && classToRun.equals("TagsCalc")) {
        	TagsCalc tagsCalc = new TagsCalc();
        	
        }else {

        	String QueryQ1 = "SELECT tags FROM `bigquery-public-data.stackoverflow.posts_questions` where tags!='' and tags is not null and creation_date > '2017-12-01' and creation_date <'2017-12-31'";
        	String QueryQ2 = "SELECT tags FROM `bigquery-public-data.stackoverflow.posts_questions` where tags!='' and tags is not null and creation_date > '2017-11-01' and creation_date <'2017-11-30'";
        	String QueryQ3 = "SELECT tags FROM `bigquery-public-data.stackoverflow.posts_questions` where tags!='' and tags is not null and creation_date > '2017-10-01' and creation_date <'2017-10-31'";
        	
        	try {
			LinkedList<TagsObj> tagsDataQ1=	BigQueryProvider.fetchRecords(QueryQ1);
			LinkedList<TagsObj> tagsDataQ2=	BigQueryProvider.fetchRecords(QueryQ2);
			LinkedList<TagsObj> tagsDataQ3=	BigQueryProvider.fetchRecords(QueryQ3);
			
			
			Dataset<TagsObj> dataset1 = sparkSession.createDataset(tagsDataQ1, Encoders.bean(TagsObj.class));
			Dataset<TagsObj> dataset2 = sparkSession.createDataset(tagsDataQ2, Encoders.bean(TagsObj.class));
			Dataset<TagsObj> dataset3 = sparkSession.createDataset(tagsDataQ3, Encoders.bean(TagsObj.class));
			
			Dataset<TagsObj> unionAll=dataset1.union(dataset2).union(dataset3);
			
			Dataset<Row> countD1 = unionAll.groupBy("tagName").count().orderBy(org.apache.spark.sql.functions.col("count").desc()).limit(20);
			
			Dataset<Row> finalData = countD1.withColumn("quarter", lit("2017-q1"));
			//finalData.coalesce(1).write().mode(SaveMode.Overwrite).csv("./final");
			String url = "jdbc:mysql://35.224.68.135:3306/stackNetwork?user=admin&password=password";
			//String url = "jdbc:mysql://localhost:3306/stackNetwork?user=root&password=12345";
//			
			finalData.coalesce(1).write().mode(SaveMode.Append).format("jdbc").option("url", url)
			.option("driver", "com.mysql.jdbc.Driver").option("dbtable","tags_analysis").save();
			
	
			
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	
}
}