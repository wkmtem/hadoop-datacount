package com.compass.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * combiner的逻辑，可以与reduce一致，也可以不一致（常用于过滤）。
 */
public class WCCombiner extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		//define a counter
		long counter = 0;
		//loop
		for(LongWritable l : values){
			counter += l.get();
		}
		//write
		context.write(key, new LongWritable(counter));
	}
}
