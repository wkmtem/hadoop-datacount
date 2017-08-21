package com.compass.hadoop.habase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @Class Name: HbaseDemo
 * @Description: Hbase Client
 * @author: wkm
 * @Company: www.compass.com
 * @Create date: 2017年7月14日下午2:07:15
 * @version: 2.0
 */
public class HbaseDemo {

	private Configuration conf = null;
	
	/**集群规划：
	 *	主机名		IP				安装的软件				          运行的进程
	 *	centos01	192.168.1.201	jdk、hadoop、hbase		          NameNode、DFSZKFailoverController、HMaster
	 *	centos02	192.168.1.202	jdk、hadoop、hbase		          NameNode、DFSZKFailoverController、HMaster
	 *	centos03	192.168.1.203	jdk、hadoop、hbase	              ResourceManager、HRegionServer
	 *	centos04	192.168.1.204	jdk、hadoop、hbase、zookeeper      DataNode、NodeManager、JournalNode、QuorumPeerMain、HRegionServer
	 *	centos05	192.168.1.205	jdk、hadoop、hbase、zookeeper      DataNode、NodeManager、JournalNode、QuorumPeerMain、HRegionServer
	 *	centos06	192.168.1.206	jdk、hadoop、hbase、zookeeper      DataNode、NodeManager、JournalNode、QuorumPeerMain、HRegionServer
	 */
	public static void main(String[] args) throws Exception {
		// hbase client
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "centos04:2181,centos05:2181,centos06:2181"); // hbase连的是zookeeper
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("tableName"));// 表描述对象
		HColumnDescriptor familyInfo = new HColumnDescriptor("info");// info列族
		familyInfo.setMaxVersions(3);// 列族的列的VERSIONS，不设置，则默认为1
		HColumnDescriptor familyData = new HColumnDescriptor("data");// data列族
		tableDesc.addFamily(familyInfo);
		tableDesc.addFamily(familyData);
		admin.createTable(tableDesc);// 创建表
		admin.close();
	}
	
	@Before
	public void init(){
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "centos04:2181,centos05:2181,centos06:2181");
	}
	
	/**
	 * 增
	 */
	@Test
	public void testPut() throws Exception{
		HTable table = new HTable(conf, "tableName"); // 获取表对象
		Put put = new Put(Bytes.toBytes("rk0001")); // put对象指定rowkey
		// 添加多属性
		put.add(Bytes.toBytes("info"), // 列族
				Bytes.toBytes("name"), // 列
				Bytes.toBytes("liuyan")); // value
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(20)); // 数字和中文会转成二进制存储
		put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes("male")); 
		table.put(put);
		table.close();
	}
	
	/**
	 * 批量增
	 */
	@Test
	public void testPutAll() throws Exception{
		HTable table = new HTable(conf, "tableName"); // 获取表对象
		List<Put> putList = new ArrayList<Put>(10000);
		Put put = null;
		for(int i = 1; i <= 1000000; i ++){
			put = new Put(Bytes.toBytes("rk" + i)); // put对象指定rowkey
			put.add(Bytes.toBytes("info"), Bytes.toBytes("menoy"), Bytes.toBytes(i + ""));
			putList.add(put);
			if(i % 10000 == 0){ // 每10000条提交一次put
				table.put(putList);
				putList = new ArrayList<Put>(10000);
			}
		}
		table.put(putList);// 最后不足10000条的put
		table.close();
	}
	
	/**
	 * 查一
	 */
	@Test
	public void testGet() throws Exception{
		HTable table = new HTable(conf, "tableName");
		Get get = new Get(Bytes.toBytes("rk1"));// get对象指定rowkey
		/*get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		get.setMaxVersions(5);*/
		Result result = table.get(get);
		String ret = Bytes.toString(result.getValue(Bytes.toBytes("info"),  // 列族
						Bytes.toBytes("menoy"))); // 列
		System.out.println(ret);
		table.close();
	}
	
	/**
	 * 查多
	 */
	@Test
	public void testScan() throws Exception{
		HTable table = new HTable(conf, "tableName");
		// 包含startRow,不包含stopRow 根据rowkey字典顺序查询，对于保持整形的自然序，rowkey必须用0作填充。
		Scan scan = new Scan(Bytes.toBytes("rk1000"), Bytes.toBytes("rk2000")); 
		scan.addFamily(Bytes.toBytes("info"));
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner){
			String ret = Bytes.toString(result.getValue(Bytes.toBytes("info"),  // 列族
					Bytes.toBytes("menoy"))); // 列
			System.out.println(ret);
			byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			System.out.println(new String(value));
		}
		table.close();
	}
	
	/**
	 * 删
	 */
	@Test
	public void testDel() throws Exception{
		HTable table = new HTable(conf, "tableName");
		Delete del = new Delete(Bytes.toBytes("rk0001")); // delete对象指定rowkey
		/*del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));*/
		table.delete(del);
		table.close();
	}
	
	/**
	 * 删除表
	 */
	@Test
	public void testDrop() throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("account"); // 先停用
		admin.deleteTable("account"); // 再删除
		admin.close();
	}
}
