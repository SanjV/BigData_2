package yelp.data;

import java.util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;

public class PaloAltoBusiness {
	public static class InMemoryJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		private ArrayList<String> businessIdList = new ArrayList<String>();

		@Override
		protected void setup(Context context) {
			try {
				Path[] localpaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for (Path myfile : localpaths) {
					FileReader fird = new FileReader(myfile.toString());
					BufferedReader buffRead = new BufferedReader(fird);
					String str = null;
					while ((str = buffRead.readLine()) != null) {
						String[] arr_values = str.split("::");
						if (arr_values[1].toLowerCase().contains("palo alto")) {
							businessIdList.add(arr_values[0]);
						}
					}

				}

			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String user_Id = arrStr[1];
			String buiss_Id = arrStr[2];
			String rate = arrStr[3];
			String str = value.toString();
			String[] arrStr = str.split("::");
			if (businessIdList.contains(buiss_Id))
				context.write(new Text(user_Id), new Text(rate));
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), conf);
		Job job = Job.getInstance(conf, "Question4");

		job.setJarByClass(PaloAltoBusiness.class);
		job.setMapperClass(InMemoryJoinMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
