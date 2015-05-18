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


public class GroupUsersByMonths {
	/**
	 * RatingsByYearsMapper reads the years and userID
	 */
	public static class RatingsByMonthsMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text yearmonth = new Text();
		private Text userID = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (!line.equals("")) {
				String[] array = line.split(",");
				// userID
				String uid = array[1];
				// read date
				String date = array[3];
				String[] dateArray = date.split("-");
				// Get the year of the ratings
				String year = dateArray[0];
				// Get the month of the ratings in certain year
				String month = dateArray[1];
				// combine the year and month into a whole string -> 199510
				String ym = year + month;
				// set ym
				yearmonth.set(ym);
				// set userID
				userID.set(uid);
				context.write(yearmonth, userID);
				
			}
			
		}
	}

	/**
	 * RatingsByYearsReducer reads the data sent by RatingsByYearsMapper and
	 * computes the average Rating of each movie
	 */
	public static class RatingsByMonthsReducer extends
			Reducer<Text, Text, Text, MyArrayWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// record number of users of each month in every year (1998 - 2005)
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

		Job average_job = new Job(conf, "Ratings by Months each Year");
		average_job.setJarByClass(GroupUsersByMonths.class);
		average_job.setMapperClass(RatingsByMonthsMapper.class);
		average_job.setMapOutputKeyClass(Text.class);
		average_job.setMapOutputValueClass(Text.class);
		average_job.setNumReduceTasks(4);
		average_job.setReducerClass(RatingsByMonthsReducer.class);
		average_job.setOutputKeyClass(Text.class);
		average_job.setOutputValueClass(MyArrayWritable.class);
		FileInputFormat.setInputPaths(average_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(average_job, new Path(args[1]));
		System.exit(average_job.waitForCompletion(true) ? 0 : 1);

	}

}
