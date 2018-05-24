package com.bim.logic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapTemp extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
	
	String line = value.toString();
	String [] chunks = line.split(" ");
	String name = chunks[0];
	int sum = 0;
	for(int i=1; i<chunks.length; i++) {
		sum= sum + Integer.parseInt(chunks[i]);
	}
	
	context.write(new Text(name), new IntWritable(sum));
	
} }