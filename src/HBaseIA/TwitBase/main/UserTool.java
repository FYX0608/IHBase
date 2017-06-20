package HBaseIA.TwitBase.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import HBaseIA.TwitBase.model.User;
import HBaseIA.TwitBase.hbase.UsersDAO;

public class UserTool {
	public static final String usage = "UsersTool action ...\n"
			+ " help - print this message and exit.\n"
			+ " add user name email password" + " - add a new user.\n"
			+ " get user - retrieve a specific user.\n"
			+ " delete - delate a specific user.\n"
			+ " list - list all installed users.\n ";

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"master:2181,slave1:2181,slave2:2181");
		HTablePool pool = new HTablePool();
		UsersDAO dao = new UsersDAO(pool);

		if (args.length == 0 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}
		

		// 添加数据
		if ("add".equals(args[0])) {
			dao.addUser(args[1], args[2], args[3], args[4]);
			User u = dao.getUser(args[1]);
			System.out.println("Successfully added user " + u);
		}

		// 读数据
		if ("get".equals(args[0])) {
			User u = dao.getUser(args[1]);
			System.out.println(u);
		}

		// 删除数据
		if ("delete".equals(args[0])) {
			dao.deleteUser(args[1]);
			System.out.println("Successfully deleted user " + args[1]);
		}

		// 列出表
		
		
		pool.closeTablePool(UsersDAO.TABLE_NAME);
	}
}
