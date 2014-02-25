package 新数据预处理;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
		urlString = "jdbc:mysql://" + "166.111.5.193:5030";

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection connection = DriverManager.getConnection(urlString, usr,
				passwd);
		int totalLength = 28608515;
		int offset = 3348000;
		int increaseStep = 1000;
		Statement statement = connection.createStatement();
		Statement statement2 = connection.createStatement();
		statement.execute("use " + database + ";");
		Statement statementInsert = connection.createStatement();

		while (offset < totalLength) {
			ResultSet resultSet = statement
					.executeQuery("SELECT * FROM meepo.time_email_ip_file limit "
							+ offset + "," + increaseStep + ";");// 查询一部分原始数据
			while (resultSet.next()) {
				String filename = resultSet.getString(4);
				String email = resultSet.getString(2);
				String sql2 = "select * FROM " + database + "." + table
						+ " where " + database + "." + table + ".email"
						+ " = '" + email + "' and " + database + "." + table
						+ ".filename = '" + filename + "' ;";

				ResultSet resultSet2 = statement2.executeQuery(sql2);
				if (resultSet2.next()) {
//					System.out.println("exists: " + filename + "," + email);
					continue;
				} else {
//					System.out.println("insert: " + filename + "," + email);
					statementInsert.executeUpdate("insert into " + table
							+ " (email,time,filename,ip) values('"
							+ resultSet.getString(2) + "','"
							+ resultSet.getString(1) + "','"
							+ resultSet.getString(4) + "','"
							+ resultSet.getString(3) + "');");
				}
			}

			offset += increaseStep;
			System.out.println("finished line:" + offset);

		}
		statement.close();
		statement2.close();
		statementInsert.close();
		connection.close();
		// select from
		// select from chakanyoumeiyou
		// ifmeiyou insert;
	}
}
