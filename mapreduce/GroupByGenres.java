package bigdata.hbase.quan.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupByGenres {
	
	/**
	 * GenresMapper reads the genres
	 */
	public static class GenresMapper extends
			Mapper<Object, Text, Text, Text> {
		
		private Text genre = new Text();
		private Text one = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// read each line
			String line = value.toString();
			String[] array = line.split(",");
			
			if (!line.equals("")) {
				
				for (int i = 1; i < array.length; i++) {
					if (!array[i].trim().equals("")) {
						genre = new Text(array[i].trim());
						String num = "1";
						one.set(num);
						context.write(genre, one);
					}
					
				}// end for
				
			}// end if

		}
		
	}
	
	/**
	 * GenresReduce calculates each genre
	 */
	public static class GenresReducer extends
		Reducer<Text, Text, Text, Text> {
		
		private Text count = new Text();
		
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				int v = Integer.parseInt(val.toString());
				sum += v;
			}
			count.set(String.valueOf(sum));
			context.write(key, count);
			
		}
		
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job genres_job = new Job(conf, "Group by Genres");
		genres_job.setJarByClass(GroupByGenres.class);
		genres_job.setMapperClass(GenresMapper.class);
		genres_job.setMapOutputKeyClass(Text.class);
		genres_job.setMapOutputValueClass(Text.class);
		// genres_job.setNumReduceTasks(1);
		genres_job.setReducerClass(GenresReducer.class);
		genres_job.setOutputKeyClass(Text.class);
		genres_job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(genres_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(genres_job, new Path(args[1]));
		System.exit(genres_job.waitForCompletion(true) ? 0 : 1);

	}

}
