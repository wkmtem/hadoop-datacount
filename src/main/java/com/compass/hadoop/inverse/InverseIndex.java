package com.compass.hadoop.inverse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 倒排索引：wordCount的变形
 */
public class InverseIndex {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InverseIndex.class);// 添加本类，作为最终运行的类
		
		// Mapper properties
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); // mapper in hdfs路径
		// Combiner local reducer
		job.setCombinerClass(IndexCombiner.class);
		// Reducer properties
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // reducer out hdfs路径
				
		// submit
		job.waitForCompletion(true);// 打印进度和详情
	}
	
	
	/**
	 * Mapper
	 */
	public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{

		private Text k = new Text(); // k2
		private Text v = new Text(); // v2
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields = line.split(" ");
			// 从context中获取输入切片（一个切片对应一个mapper）
			FileSplit inputSplit = (FileSplit) context.getInputSplit();// InputSplit抽象类的子类
			// 获取路径
			Path path = inputSplit.getPath();
			// 获取文件名
			String name = path.getName();
			for(String s : fields){
				k.set(s + "->" + name);
				v.set("1");
				context.write(k, v); // "hello->a.txt", "1"
			}
		}
	}
	
	
	/**
	 * Combiner
	 * combiner与reducer逻辑不同时，则不能部署在集群环境中
	 */
	public static class IndexCombiner extends Reducer<Text, Text, Text, Text>{

		private Text k = new Text(); // k2
		private Text v = new Text(); // v2
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] wordAndPath = key.toString().split("->");
			long counter = 0;
			for(Text t : values){
				counter += Integer.parseInt(t.toString());
			}
			k.set(wordAndPath[0]); // word
			v.set(wordAndPath[1] + "->" + counter); // path
			context.write(k, v); // "hello", "a.txt->3"
		}
	}
	
	
	/**
	 * Reducer
	 * combiner的输出k2，v2，是reduce的输入
	 */
	public static class IndexReducer extends Reducer<Text, Text, Text, Text>{

		private Text v = new Text(); // v3
		
		@Override
		protected void reduce(Text k2, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String v3 = null;
			for(Text t : values){
				v3 += t.toString() + " ";
			}
			v.set(v3);
			context.write(k2, v);
		}
	}
}
