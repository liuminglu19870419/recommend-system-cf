package 为数据建立索引;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		String urlString = "";// 数据库连接命令
		String usr = "meepo";// 用户名
		String passwd = "meepo";// 密码
		String driverString = "com.mysql.jdbc.Driver";// 连接驱动命令
		String database = "test";
		String table = "teif_for11to9";
		String table1[] = {"teif_1","teif_2","teif_3"};
		urlString = "jdbc:mysql://" + "166.111.5.193:5030";

		Class.forName(driverString).newInstance();
		Connection connection = DriverManager.getConnection(urlString, usr,
				passwd);
		int totalLength = 15942764;//28608515;
		int offset = 0;
		int increaseStep = 10000;
		Statement statement = connection.createStatement();
		statement.execute("use " + database + ";");

		while (offset < totalLength) {
			offset += increaseStep;
			statement.executeUpdate("insert into " + table1[(offset / 10000) % 3]
					+ " select  * FROM " + table + " limit " + offset + ","
					+ increaseStep + ";");
			System.out.println("finished line:" + offset);

		}
		statement.close();
		connection.close();
		// select from
		// select from chakanyoumeiyou
		// ifmeiyou insert;
	}
}
