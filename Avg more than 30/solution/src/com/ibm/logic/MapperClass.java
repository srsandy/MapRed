package com.ibm.logic;

import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.StringTokenizer; 



public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

	String line = value.toString();
	String lasttoken = null; 
	
	StringTokenizer s = new StringTokenizer(line," "); 
    
	String year = s.nextToken();
	
	while(s.hasMoreTokens()) {
		lasttoken = s.nextToken();
	}
	
	int avgTemp = Integer.parseInt(lasttoken); 
     
	context.write(new Text(year), new IntWritable(avgTemp)); 


	
	
} }
