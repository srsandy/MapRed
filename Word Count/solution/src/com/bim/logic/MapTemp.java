package com.bim.logic;

import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;



public class MapTemp extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

	String line = value.toString();
	String [] chunks = line.split(" ");
	
	for(String s: chunks) {
		context.write(new Text(s), new IntWritable(1)); 
	}
	
} }