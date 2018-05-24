package com.bim.logic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedTemp extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override

public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
	int avg = Integer.MIN_VALUE;
	for (IntWritable value: values) {
		avg = value.get()/5;
	}
	context.write(key, new IntWritable(avg));
	
} }