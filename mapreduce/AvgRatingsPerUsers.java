package bigdata.hbase.quan.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgRatingsPerUsers {
	
	/**
	 * RatingsByYearsMapper reads the years and userID
	 */
	public static class AvgRatingsPerUserMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text userID = new Text();
		private Text rating = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (!line.equals("")) {
				String[] array = line.split(",");
				String uid = array[1];
				String r = array[2];
				// Get the userID of the ratings
				userID.set(uid);
				rating.set(r);
				context.write(userID, rating);
				
			}
			
		}
	}

	/**
	 * RatingsByYearsReducer reads the data sent by RatingsByYearsMapper and
	 * computes the average Rating of each movie
	 */
	public static class AvgRatingsPerUserReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {
		
		double avg = 0.00d;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sumRating = 0;
			// record number of ratings by each user
			int sum = 0;
			// add all the ratings for each user
			ArrayList<Integer> ratingsPerUser = new ArrayList<Integer>(); 
			for (Text val: values) {
				ratingsPerUser.add(new Integer(Integer.parseInt(val.toString())));
				sum++;
			}
			// key->userID, sum->Movies Number for each user
			String str = key + "	" + sum;
			// calculate the average rating
			for (int rating: ratingsPerUser) {
				sumRating += rating;
			}
			avg = (double)((double)sumRating / (double)ratingsPerUser.size());
			DecimalFormat df = new DecimalFormat("#.##");
			double arerage = Double.valueOf(df.format(avg));
			key.set(str);
	        
	        context.write(key, new DoubleWritable(arerage));

		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job average_job = new Job(conf, "Avg rating per user");
		average_job.setJarByClass(AvgRatingsPerUsers.class);
		average_job.setMapperClass(AvgRatingsPerUserMapper.class);
		average_job.setMapOutputKeyClass(Text.class);
		average_job.setMapOutputValueClass(Text.class);
		average_job.setNumReduceTasks(4);
		average_job.setReducerClass(AvgRatingsPerUserReducer.class);
		average_job.setOutputKeyClass(Text.class);
		average_job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(average_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(average_job, new Path(args[1]));
		System.exit(average_job.waitForCompletion(true) ? 0 : 1);

	}
	
	

}
