import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
		
	public static void main(String[] args) {
		String sourceTable=args[0];
		String targetTable=args[1];
		String classToRun=args[2];
		
		SparkSession sparkSession = SparkSession.builder().master("local").appName("stack-exchange-analysis")
				                    .getOrCreate();
		
        if(classToRun.equals("ProcessQueAndAns")) {
		ProcessQueAndAns countQueAndAns = new ProcessQueAndAns();
		countQueAndAns.countQueAndAns(sparkSession, sourceTable, targetTable);
		}
        else if(classToRun.equals("TagsCalc")) {
        	TagsCalc tagsCalc = new TagsCalc();
        	
        }
	
	

}
}