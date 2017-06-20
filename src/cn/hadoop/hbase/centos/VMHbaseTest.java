package cn.hadoop.hbase.centos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;;

public class VMHbaseTest {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"master:2181,slave1:2181,slave2:2181");
		
		HBaseAdmin admi = new HBaseAdmin(conf);
		TableName tableName=TableName.valueOf("fyx");
		HTableDescriptor htableDescriptor = new HTableDescriptor(tableName);
		//htableDescriptor.addFamily(new HColumnDescriptor("data1"));
		admi.createTable(htableDescriptor);
        System.out.println("创建成功");
	}
}
