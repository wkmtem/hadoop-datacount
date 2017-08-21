package com.compass.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce 1:无自定义排序
 */
public class SumStep {

	/**
	 * 自定义map与自定义reduce组装
	 * hadoop jar <jar在linux的路径> <main方法所在的类的全类名> <参数>
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SumStep.class); // 设置jar包启动main方法所在的类
		// map properties
		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));// map读取需计算的数据hdfs地址（文件或者文件夹，不会读取文件夹中_开始的文件）
		// reduce properties
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// reduce写出计算数据的结果hdfs地址
		// 提交作业，并打印进度和详情
		job.waitForCompletion(true);
	}
	

	/**
	 * 自定义mapper
	 * k1:偏移量，v1:一行内容，k2:map输出，v2:map输出
	 */
	public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean>{

		private Text k = new Text();// map输出k2 全局变量
		private InfoBean v = new InfoBean();// map输出v2 全局变量
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// split 
			String line = value.toString();
			String[] fields = line.split("\t");
			
			// get useful field
			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);
			k.set(account);
			v.set(account, income, expenses);
			context.write(k, v);
		}
	}
	
	
	/**
	 * 自定义 reducer
	 * k2:map的输出，v2:map的输出，k3:reducer的输出，v3:reducer的输出
	 * k3 == k2
	 */
	public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean>{

		private InfoBean v = new InfoBean();// reducer输出v3 全局变量
		
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values, Context context)
				throws IOException, InterruptedException {
			
			double in_sum = 0;
			double out_sum = 0;
			
			for(InfoBean bean : values){
				in_sum += bean.getIncome();
				out_sum += bean.getExpenses();
			}
			v.set("", in_sum, out_sum);
			context.write(key, v);
		}
	}
}
