package com.compass.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce 2:自定义排序，对SumStep的reduce结果进行再次计算
 */
public class SortStep {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortStep.class);// 设置jar包启动main方法所在的类
		// map properties
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));// map读取需计算的数据hdfs地址（文件或者文件夹，不会读取文件夹中_开始的文件）
		// reduce properties
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// reduce写出计算数据的结果hdfs地址
		// 提交作业，并打印进度和详情
		job.waitForCompletion(true);
	}
	
	/**
	 * k1:偏移量，v1:一行内容，k2:框架会自动排序，自定义排序，则在这里传入需要排序的对象，v2:无输出
	 */
	public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable>{

		private InfoBean k = new InfoBean();// map输出k2 全局变量
		
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, InfoBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			k.set(
					fields[0], 
					Double.parseDouble(fields[1]), 
					Double.parseDouble(fields[2])
				);
			// 对象作为k2，则根据bean中的compareTo方法排序，不重写compareTo方法，则按照默认的框架排序
			context.write(k, NullWritable.get());
		}
	}
	
	/**
	 * 自定义 reducer
	 * k2:map的输出，v2:map的输出，k3:reducer的输出，v3:reducer的输出
	 * v3 == k2
	 */
	public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean>{

		private Text k = new Text();// reducer输出k3 全局变量
		
		@Override
		protected void reduce(InfoBean key, Iterable<NullWritable> values,
				Reducer<InfoBean, NullWritable, Text, InfoBean>.Context context)
				throws IOException, InterruptedException {
			
			k.set(key.getAccount());
			// mapper已完成自定义排序，直接写出去即可
			context.write(k, key);
		}
	}
}
