package HBaseIA.TwitBase.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.Processor.put;
import org.apache.hadoop.hbase.util.Bytes;

public class UsersDAO {
	// 声明一次常用的字节数组常量
	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	public static final byte[] INFO_FAMILY = Bytes.toBytes("info");

	private static final byte[] USER_COL = Bytes.toBytes("user");
	private static final byte[] NAME_COL = Bytes.toBytes("name");
	private static final byte[] EMAIL_COL = Bytes.toBytes("email");
	private static final byte[] PASS_COL = Bytes.toBytes("password");

	public static final byte[] TWEETS_COL = Bytes.toBytes("tweets_count");

	private HTablePool pool ;

	// 让调用环境来管理连接池
	public UsersDAO(HTablePool pool) {
		this.pool = pool;
	}

	private static Get mkGet(String user) {
		Get g = new Get(Bytes.toBytes(user));//
		g.addFamily(INFO_FAMILY);
		return g;
	}

	private static Put mkPut(User u) {
		Put p = new Put(Bytes.toBytes(u.user));
		p.add(INFO_FAMILY, USER_COL, Bytes.toBytes(u.user));
		p.add(INFO_FAMILY, NAME_COL, Bytes.toBytes(u.name));
		p.add(INFO_FAMILY, EMAIL_COL, Bytes.toBytes(u.email));
		p.add(INFO_FAMILY, PASS_COL, Bytes.toBytes(u.password));
		return p;
	}

	private static Delete mkDelete(String user) {
		Delete d = new Delete(Bytes.toBytes(user));
		return d;
	}

	public void addUser(String user, String name, String email, String password)
			throws IOException {
		HTableInterface usersTable = pool.getTable(TABLE_NAME);
		User u = new User(user, name, email, password);
		Put p = mkPut(u);
		usersTable.put(p);
	}

	public User getUser(String user) throws IOException {
		HTableInterface usersTable = pool.getTable(TABLE_NAME);
		Get g = mkGet(user);
		Result result = usersTable.get(g);
		if (result.isEmpty()) {
			System.out.println("");
		}
		User u = new User(result);
		usersTable.close();
		return u;
	}

	public void deleteUser(String user) throws IOException {
		HTableInterface usersTable = pool.getTable(TABLE_NAME);
		Delete d = mkDelete(user);
		usersTable.delete(d);
		usersTable.close();
	}
	
	private static class User extends HBaseIA.TwitBase.model.User {

		private long tweetCount;

		private User(Result r) {
			this(r.getValue(INFO_FAMILY, USER_COL), r.getValue(INFO_FAMILY,
					NAME_COL), r.getValue(INFO_FAMILY, EMAIL_COL), r.getValue(
					INFO_FAMILY, PASS_COL),
					r.getValue(INFO_FAMILY, TWEETS_COL) == null ? Bytes
							.toBytes(0l) : r.getValue(INFO_FAMILY, TWEETS_COL));

		}

		private User(byte[] user, byte[] name, byte[] email, byte[] password,
				byte[] tweetCount) {
			this(Bytes.toString(user), Bytes.toString(name), Bytes
					.toString(email), Bytes.toString(password));
			this.tweetCount = Bytes.toLong(tweetCount);
		}

		public User(String user, String name, String email, String password) {
			this.user=user;
			this.name=name;
			this.email=email;
			this.password=password;
		}
	}

	 
}
