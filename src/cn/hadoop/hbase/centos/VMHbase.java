package cn.hadoop.hbase.centos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class VMHbase {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"master:2181,slave1:2181,slave2:2181");
		HTablePool pool = new HTablePool();
		HTableInterface usertable = pool.getTable("users");
		// Put p = new Put(Bytes.toBytes("TheRealMT"));
		// p.add(Bytes.toBytes("info"), Bytes.toBytes("name"),
		// Bytes.toBytes("Mark"));
		// p.add(Bytes.toBytes("info"), Bytes.toBytes("email"),
		// Bytes.toBytes("1449038397@qq.com"));
		// p.add(Bytes.toBytes("info"), Bytes.toBytes("password"),
		// Bytes.toBytes("asdfghjkl"));
		// usertable.put(p);
		// 插入数据
		MyPut("TheRealMT", "info", "name", "Mark", usertable);
		MyPut("TheRealMT", "info", "email", "1449038397@qq.com", usertable);
		MyPut("TheRealMT", "info", "password", "asdfghjkl", usertable);

		// 修改数据
		MyPut("TheRealMT", "info", "password", "abc123", usertable);

		//读数据
		MyGet("TheRealMT",usertable);
		//MyGet("TheRealMT","info", "password", usertable);
		
		
		System.out.println("成功");
		usertable.close();
		// HBaseAdmin admi = new HBaseAdmin(conf);
		// TableName tableName=TableName.valueOf("fyx");
		// HTableDescriptor htableDescriptor = new HTableDescriptor(tableName);
		// //htableDescriptor.addFamily(new HColumnDescriptor("data1"));
		// admi.createTable(htableDescriptor);

	}

	/**
	 * 存储数据 修改数据
	 * 
	 * @param rowkey
	 * @param columnfamily
	 * @param column
	 * @param value
	 * @param usertable
	 */
	public static void MyPut(String rowkey, String columnfamily, String column,
			String value, HTableInterface usertable) {
		try {
			Put p = new Put(Bytes.toBytes(rowkey));
			p.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column),
					Bytes.toBytes(value));
			usertable.put(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读数据
	 * 
	 * @param rowkey
	 * @param columnfamily
	 * @param column
	 * @param usertable
	 */
	public static void MyGet(String rowkey, String columnfamily, String column,
			HTableInterface usertable) {
		try {
			Get g = new Get(Bytes.toBytes(rowkey));
			g.addFamily(Bytes.toBytes(columnfamily));
			Result r = usertable.get(g);
			byte[] b = r.getValue(Bytes.toBytes(columnfamily),
					Bytes.toBytes(column));
			System.out.println(Bytes.toString(b));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void MyGet(String rowkey, HTableInterface usertable) {
		try {
			Get g = new Get(Bytes.toBytes(rowkey));
			g.addFamily(Bytes.toBytes("info"));
			Result r = usertable.get(g);
			System.out.println(r.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
