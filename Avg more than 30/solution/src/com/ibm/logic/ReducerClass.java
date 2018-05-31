package com.ibm.logic;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override

public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
	
	int maxavg=30; 
	int val = Integer.MIN_VALUE;

	for (IntWritable value: values) {
		if((val=value.get())>maxavg) 
			context.write(key, new IntWritable(val));
	}

} }
