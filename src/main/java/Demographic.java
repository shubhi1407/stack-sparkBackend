import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Demographic {
		
	public static void main(String[] args) {
		String url = "jdbc:mysql://35.224.68.135:3306/stackNetwork?user=admin&password=password";
		SparkSession sparkSession = SparkSession.builder().appName("stack-exchange-analysis")
				.getOrCreate();

		Dataset<Post> df = sparkSession.read().format("jdbc").option("url", url)
				.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "posts_questions").load()
				.as(Encoders.bean(Post.class));

		// Looks the schema of this DataFrame.
		df.printSchema();

		df = df.filter(removeEmptyLocations)
				.map(mapLocation, Encoders.bean(Post.class));
		
		Dataset<Row> countDF = df.groupBy("Location").count();
		
		countDF.filter(countDF.col("count").gt(3)).coalesce(1).write().mode(SaveMode.Append).format("jdbc").option("url", url)
		.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "demographic").save();
		
		//countDF.filter(countDF.col("count").gt(3)).coalesce(1).write().mode(SaveMode.Overwrite).csv("./count");
		
		//countDF.write().mode(SaveMode.Overwrite).csv("./output.csv");
		//System.out.println(df.count());
		
		sparkSession.close();
	}
	
	static FilterFunction<Post> removeEmptyLocations = post -> {
		return post.getLocation() != null && post.getLocation().length() > 0;
	};
	
	
	static MapFunction<Post,Post> mapLocation = p -> {
		String originalLocation = p.getLocation();
		
		if(originalLocation.contains(",")) {
			String[] tokens = originalLocation.split(",");
			String state = tokens[tokens.length-1].trim();
			for(String USstate : GeographicalData.USstates) {
				if(USstate.equals(state)) {
					p.setLocation("United States");
					return p;
				}
			}
		}
		for (String country : GeographicalData.countries) {
			if (originalLocation.toLowerCase().contains(country.toLowerCase())) {
				p.setLocation(country);
				return p;
			}
		}
		for(String USStateFull : GeographicalData.states) {
			if(originalLocation.toLowerCase().contains(USStateFull.toLowerCase())) {
				p.setLocation("United States");
				return p;
			}
		}
		for(String ind : GeographicalData.india) {
			if(originalLocation.toLowerCase().contains(ind.toLowerCase())) {
				p.setLocation("India");
				return p;
			}
		}
		for(String u : GeographicalData.uk) {
			if(originalLocation.toLowerCase().contains(u.toLowerCase())) {
				p.setLocation("United Kingdom");
				return p;
			}
		}
		for(String ru: GeographicalData.russia) {
			if(originalLocation.toLowerCase().contains(ru.toLowerCase())) {
				p.setLocation("Russian Federation");
				return p;
			}
		}
		for(String au : GeographicalData.aus) {
			if(originalLocation.toLowerCase().contains(au.toLowerCase())) {
				p.setLocation("Australia");
				return p;
			}
		}
		for(String pk : GeographicalData.pakistanCities) {
			if(originalLocation.toLowerCase().contains(pk.toLowerCase())) {
				p.setLocation("Pakistan");
				return p;
			}
		}
		for(String mx : GeographicalData.mexico) {
			if(originalLocation.toLowerCase().contains(mx.toLowerCase())) {
				p.setLocation("Mexico");
				return p;
			}
		}
		if(originalLocation.toLowerCase().contains("korea"))
			p.setLocation("Korea, Republic of");
		if(originalLocation.toLowerCase().contains("serbia"))
			p.setLocation("Serbia");
		return p;
	};
	
	

}
