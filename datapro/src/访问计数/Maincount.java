package 访问计数;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Maincount {
	public static void main(String[] args) throws FileNotFoundException {
		String urlString = "";// 数据库连接命令
		String usr = "meepo";// 用户名
		String passwd = "meepo";// 密码
		String driverString = "com.mysql.jdbc.Driver";// 连接驱动命令
		urlString = "jdbc:mysql://" + "166.111.5.193:5030";

		PrintWriter print = new PrintWriter(new File("cout.txt"));
		
		
		try {
			Class.forName(driverString).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection connection;
		try {
			connection = DriverManager.getConnection(urlString, usr, passwd);
			Statement statement;
			statement = connection.createStatement();
			statement.execute("use test;");
			int step = 50;
			int start = 0;
			int end = start + step;
			int count = 100;
//			while (count != 0) {
//				ResultSet resultSet = statement
//						.executeQuery("select count(*) from fileset where count >= "
//								+ start + " and count < " + end + ";");
//				count = 0;
//				if (resultSet.next()) {
//					count = resultSet.getInt(1);
//					System.out.println(resultSet.getInt(1) + " " + start + "~"
//							+ end);
//					print.println(resultSet.getInt(1));
//				}
//				start = end;
//				end = start + step;
//			}
			step = 20;
			start = 0;
			end = start + step;
			count = 100;
			while (count != 0) {
				ResultSet resultSet = statement
						.executeQuery("select count(*) from userset where count >= "
								+ start + " and count < " + end + ";");
				count = 0;
				if (resultSet.next()) {
					count = resultSet.getInt(1);
					System.out.println(resultSet.getInt(1) + " " + start + "~"
							+ end);
					print.println(resultSet.getInt(1));
				}
				start = end;
				end = start + step;
			}
			print.flush();
			print.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
