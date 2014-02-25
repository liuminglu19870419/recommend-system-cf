package 生成用户集合;

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
		String table = "userset";
		String table1[] = { "teif_1", "teif_2", "teif_3" };
		urlString = "jdbc:mysql://" + "166.111.5.193:5030";

		Class.forName(driverString).newInstance();
		Connection connection = DriverManager.getConnection(urlString, usr,
				passwd);
		int totalLength = 3918243;
		int offset = 0;
		int increaseStep = 10000;
		Statement statement = connection.createStatement();
		Statement statement2 = connection.createStatement();
		statement.execute("use " + database + ";");

		while (offset < totalLength) {
			ResultSet resultSet = statement.executeQuery("select email FROM "
					+ table + " limit " + +offset + "," + increaseStep + ";");
			while (resultSet.next()) {
				int count = 0;
				for (String string : table1) {
					ResultSet resultSet2 = statement2
							.executeQuery("select  count(*) FROM " + string
									+ " where email = '"
									+ resultSet.getString(1) + "';");
					resultSet2.next();
					count += Integer.parseInt(resultSet2.getString(1));
				}
				statement2.executeUpdate("update userset set `count` = "
						+ count + " where email = '" + resultSet.getString(1)
						+ "'");
				// System.out.println(resultSet.getString(1) + ":" + count);
			}
			offset += increaseStep;
			System.out.println("finished line:" + offset);

		}
		statement.close();
		connection.close();
		// select from
		// select from chakanyoumeiyou
		// ifmeiyou insert;
	}
}