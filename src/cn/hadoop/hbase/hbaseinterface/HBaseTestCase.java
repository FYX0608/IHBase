package cn.hadoop.hbase.hbaseinterface;
/**
 * 连接服务器的正确的方法
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseTestCase{
	private static final String TableName = "fyx";
	private static final String FamilyName = "f2";

	public static void main(String[] args) throws Exception{
		Configuration conf =HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "slave1:2181,slave2:2181,slave3:2181");
		//conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
		//api:HbaseAdmin
		HBaseAdmin admin=new HBaseAdmin(conf);
		HTableDescriptor htableDescriptor =new HTableDescriptor(TableName);
		htableDescriptor.addFamily(new HColumnDescriptor(FamilyName));
		admin.createTable(htableDescriptor );
		
	}
}
