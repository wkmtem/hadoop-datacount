package com.compass.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * hadoop没有使用jdk默认的序列化机制（继承关系）代码冗余，效率太低，hadoop只需要传递数据，使用自己的序列化机制
 * Long --> LongWritable, String --> Text 
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//accept
		String line = value.toString();
		//split
		String[] words = line.split(" ");
		//loop
		for(String w : words){
			//send
			context.write(new Text(w), new LongWritable(1));
		}
	}
}
