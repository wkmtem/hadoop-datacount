package com.compass.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFS_HA {

	/**
	 * 完成hadoop分布式搭建后，对hdfs的测试
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://ns1"); 
		// 设置hdfs-site.xml的configuration中property的name/value配置
		conf.set("dfs.nameservices", "ns1"); // nameservices的名称
		conf.set("dfs.ha.namenodes.ns1", "nn1,nn2"); // nameservices下的namenode
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "centos01:9000"); // rpc通信地址
		conf.set("dfs.namenode.rpc-address.ns1.nn2", "centos02:9000"); // rpc通信地址
		// 失败自动切换实现方式
		conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		//conf.setBoolean(name, value);
		FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root");// 连接namenode的抽象类：nameservices，root：伪装用户
		
		// 测试下载
		InputStream dlin = fs.open(new Path("/log")); // 下载log文件
		OutputStream dlout = new FileOutputStream("d://123456789.txt"); // 写入本地的输出流
		IOUtils.copyBytes(dlin, dlout, 4096, true); // copy后，关闭流
		
		// 测试上传
		InputStream ulin = new FileInputStream("d://eclipse.rar"); // 上传本地的输入流
		OutputStream ulout = fs.create(new Path("/eclipse")); // 写入hdfs的输出流
		IOUtils.copyBytes(ulin, ulout, 4096, true);
		
		/**
		 * 测试cordcount
		 * 1.将需计算的文件上传至HDFS(在文件所在的文件夹中)根目录下：hdfs dfs -put 文件名 /
		 * 2.hadoop jar /cloud/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar wordcount /HDFS输入文件名 /HDFS输出文件名
		 */
	}
}
