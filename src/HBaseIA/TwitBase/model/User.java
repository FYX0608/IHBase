package HBaseIA.TwitBase.model;

import org.apache.hadoop.hbase.client.Result;



/**
 * User的数据模型
 * @author FYX
 */
public abstract class User {
	public String user;
	public String name;
	public String email;
	public String password;
	
	public String toString() {
		return String.format("<User:%s,%s,%s>", user, name, email);
	}
}


