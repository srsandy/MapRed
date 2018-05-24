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
	String phone = chunks[0];
	int amt = Integer.parseInt(chunks[chunks.length-1]);
	
	context.write(new Text(phone), new IntWritable(amt)); 
	
} }