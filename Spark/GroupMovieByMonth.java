import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class GroupMovieByMonth {
	
	public static void main(String[] args) {
		// String master = "spark://localhost:7077";
		String master = "local";
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("GroupMovieByMonth").setMaster(
				master);
		;
		JavaSparkContext spark = new JavaSparkContext(conf);
		// Load our input data.
		// JavaRDD<String> input = sc.textFile(inputFile);

		JavaRDD<String> file = spark
				.textFile("hdfs://localhost:9000/user/dataset/movie_ratings.txt");
		System.out.println(file);

		// Split up into words.
		JavaRDD<String> words = file
				.flatMap(new FlatMapFunction<String, String>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						String[] entry = x.split(",");
						String yearmonth = entry[3];
						String[] entry1 = yearmonth.split("-");
						String year = entry1[0];
						String month = entry1[1];
						String result = year + month;						
						return Arrays.asList(result);
						
					}
				});
		// Transform into yearmonth and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
		
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String x) {
						return new Tuple2(x, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile("hdfs://localhost:9000/user/output/3");

	}


}
