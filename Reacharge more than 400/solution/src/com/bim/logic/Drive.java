package com.bim.logic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.bim.logic.MapTemp;
import com.bim.logic.RedTemp;
public class Drive {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] programArgs =
				new GenericOptionsParser(conf, args).getRemainingArgs();
		if (programArgs.length != 2) {
			System.err.println("Usage: Rechager more than 400 <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Rechager more than 400");
		job.setJarByClass(Drive.class);
		job.setMapperClass(MapTemp.class);
		//job.setCombinerClass(RedTemp.class);
		job.setReducerClass(RedTemp.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(programArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));
		// Submit the job and wait for it to finish.
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
