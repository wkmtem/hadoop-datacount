package com.compass.hadoop.mr.dc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * JAR file 打jar包
 * Export all output folders for checked projects 只打包本项目的jar包（勾选第1个，则添加第三方的jar）
 * Select the export destination 指定jar包路径
 * hdfs dfs -put 需计算的文件名 /hdfs目录/文件名称	上传需要计算的文件到hdfs系统
 * hadoop jar <jar在linux的路径> <main方法所在的类的全类名> <参数>
 * hadoop jar /root/examples.jar com.comapss.hadoop.mr.dc.DataCount /HDFS文件读取地址(args[0]) /HDFS文件写入地址目录(args[1])
 * hadoop jar /root/examples.jar com.comapss.hadoop.mr.dc.DataCount /hdfs://centos:9000/data.doc /hdfs://centons:9000/dataout
 */
public class DataCount {
	
	// main方法3个参数：文件输入地址，结果输出地址，reduce数量
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();// 读取全局xml文件配置
		//conf.set(name, value);// 对MapReduce个性化配置
		// 设置replication复制副本数量（hadoop-mapreduce-client-core的mapred-默认10个副本，读取不到，则使用后面定义的数量）
		//conf.setInt("mapreduce.client.submit.file.replication", 20);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(DataCount.class);// main方法所在的类，即本类
		// map properties
		job.setMapperClass(DCMapper.class);
		job.setMapOutputKeyClass(Text.class);// 在满足（k2 v2 与 k3 v3的类型一一对应）条件时，可省略
		job.setMapOutputValueClass(DataBean.class);// 同上
		FileInputFormat.setInputPaths(job, new Path(args[0]));// 文件的输入流（读取HDFS）
		// 组装Partitioner分区
		job.setPartitionerClass(DCPartitioner.class);
		// 设置 reduce的数量，默认只启动1个
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		// reduce properties
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// reduce 输出流（写入HDFS）

		job.waitForCompletion(true);// 提交计算作业，并打印进度和详情
	}
	

	/**
	 * 自定义Mapper 
	 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
	 * Mapper<k1, v1, k2, v2>
	 */
	public static class DCMapper extends
			Mapper<LongWritable, Text, Text, DataBean> {

		@Override
		// key：字符偏移量，value：一行内容，包括换行符
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String telNo = null;
			long up = 0L;
			long down = 0L;

			// accept
			String line = value.toString();// 获取一行内容

			// split
			String[] fields = line.split("\t");
			try {
				telNo = fields[1];
				up = Long.parseLong(fields[8]);// 上行
				down = Long.parseLong(fields[9]);// 下行
			} catch (Exception e) {
				e.printStackTrace();
			}
			DataBean bean = new DataBean(telNo, up, down);

			// send，自动按照k2分组
			context.write(new Text(telNo), bean);// new Text()只使用一次，效率低，定义全局变量后赋值
		}
	}

	
	/**
	 * map与reduce之间的过渡
	 * 自定义Partitioner分区
	 * Partitioner<KEY, VALUE>
	 * map的输出：k2，v2，是Partitioner<k2, v2>输入
	 * 系统有默认的HashPartitioner：
	 * public int getPartintion(K key, V value, int numReduceTasks) {
	 * 		return (key.hashCode() & Integer.Max_VALUE) % numReduceTasks;
	 * }
	 */
	public static class DCPartitioner extends  Partitioner<Text, DataBean>{
		
		// 模拟数据
		private static Map<String, Integer> provider = new HashMap<String, Integer>();
		static {
			provider.put("138", 1);
			provider.put("139", 1);
			provider.put("152", 2);
			provider.put("153", 2);
			provider.put("182", 3);
			provider.put("183", 3);
		}
		
		// 返回的int值，对应结果文件的序号
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			//向数据库或配置信息 读写
			String tel_sub = key.toString().substring(0, 3); // 截取key（账号）的前位
			Integer code = provider.get(tel_sub);
			if(code == null){
				code = 0;
			}
			return code;
		}
	}
	
	
	/**
	 * 自定义Reducer 
	 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
	 * Reducer<k2, v2, k3, v3>
	 */
	public static class DCReducer extends
			Reducer<Text, DataBean, Text, DataBean> {

		@Override
		protected void reduce(Text key, Iterable<DataBean> values,
				Context context) throws IOException, InterruptedException {
			
			long up_sum = 0;// 上行计数器
			long down_sum = 0;// 下行计数器
			
			for (DataBean bean : values) {
				up_sum += bean.getUpPayLoad();
				down_sum += bean.getDownPayLoad();
			}
			DataBean bean = new DataBean("", up_sum, down_sum);
			context.write(key, bean);// 手机号，（上行，下行，总流量）
		}
	}
}
