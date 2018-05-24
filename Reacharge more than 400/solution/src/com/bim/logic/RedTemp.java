package com.bim.logic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedTemp extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override

public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
	int min = 400;
	int val = Integer.MIN_VALUE;
	for (IntWritable value: values) {
		val = value.get();
		if(val >= min) {
			context.write(key, new IntWritable(val));
		}
	}
	
	
} }