package bigdata.hbase.quan.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupUsersByYears {
	
	/**
	 * RatingsByYearsMapper reads the years and userID
	 */
	public static class RatingsByYearsMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text year = new Text();
		private Text userID = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (!line.equals("")) {
				String[] array = line.split(",");
				String uid = array[1];
				String date = array[3];
				String[] dateArray = date.split("-");
				// Get the year of the ratings
				String yearstr = dateArray[0];
				year.set(yearstr);
				userID.set(uid);
				context.write(year, userID);
				
			}
			
		}
	}

	/**
	 * RatingsByYearsReducer reads the data sent by RatingsByYearsMapper and
	 * computes the average Rating of each movie
	 */
	public static class RatingsByYearsReducer extends
			Reducer<Text, Text, Text, MyArrayWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// record number of users of each year (1998 - 2005)
			int sum = 0;
			ArrayList<Text> list = new ArrayList<Text>(); 
			for (Text val: values) {
				list.add(new Text(val));
				sum++;
			}
			String str = key + "	" + sum;
			key.set(str);
	        
	        context.write(key, new MyArrayWritable(Text.class,list.toArray(new Text[list.size()])));
			//context.write(new Text(key), new DoubleWritable(arerage));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job average_job = new Job(conf, "Ratings by Years");
		average_job.setJarByClass(GroupUsersByYears.class);
		
		average_job.setMapperClass(RatingsByYearsMapper.class);
		average_job.setMapOutputKeyClass(Text.class);
		average_job.setMapOutputValueClass(Text.class);
		average_job.setNumReduceTasks(4);
		average_job.setReducerClass(RatingsByYearsReducer.class);
		average_job.setOutputKeyClass(Text.class);
		average_job.setOutputValueClass(MyArrayWritable.class);
		FileInputFormat.setInputPaths(average_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(average_job, new Path(args[1]));
		System.exit(average_job.waitForCompletion(true) ? 0 : 1);

	}

}
