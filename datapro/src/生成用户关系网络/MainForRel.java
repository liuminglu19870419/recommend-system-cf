package 生成用户关系网络;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.xml.crypto.dsig.spec.ExcC14NParameterSpec;

import org.omg.CORBA.PUBLIC_MEMBER;

public class MainForRel {

	static final boolean debug = false;// 控制是否输出debug临时信息
	static final String[] tableStrings = { "userrel1", "userrel2", "userrel3",
			"userrel4", "userrel5", "userrel6", "userrel7", "userrel8",
			"userrel9", "userrel10" };// 用户关系表
	static final String[] resTableStrings = { "teif_1", "teif_2", "teif_3" };// 原来的访问log
	static Map<String, Queue<nodeEmailCount>> resultBuffer;// 存储临时信息的缓冲记录集
	static final int MAXSIZE = 1024;
	static final int THREHOLD = 10;// 共同文件阈值下限

	static final String urlString = "jdbc:mysql://" + "166.111.5.193:5030";// 数据库连接命令
	static final String usr = "meepo";// 用户名
	static final String passwd = "meepo";// 密码
	static final String driverString = "com.mysql.jdbc.Driver";// 连接驱动命令
	static final String database = "test";

	// 临时关系对
	private static class nodeEmailCount implements Serializable {
		String email;
		String emailRelv;
		int commonFileCount;

		public nodeEmailCount(String email, String emailRelv,
				int commonFileCount) {
			this.email = email;
			this.emailRelv = emailRelv;
			this.commonFileCount = commonFileCount;
		}
	}

	static {
		resultBuffer = new HashMap<String, Queue<nodeEmailCount>>();
		for (String string : tableStrings) {
			Queue<nodeEmailCount> queue = new ConcurrentLinkedQueue<MainForRel.nodeEmailCount>();
			resultBuffer.put(string, queue);
		}
	}

	public MainForRel() throws InterruptedException {
		if (debug) {
			System.out.println("Main() called");
			Thread.sleep(1000);
		}
	}

	/**
	 * 使用hash值得绝对值计算email归属的表
	 * 
	 * @author xiaoluBambi
	 * @param email
	 * @return 该用户所属的表
	 */
	static String calcHashedTable(String email) {
		return tableStrings[Math.abs(email.hashCode()) % tableStrings.length];
	}

	/**
	 * 计算共同访问文件数
	 * 
	 * @author xiaoluBambi
	 * @param fileSetforEmail1
	 *            用户1的全部访问文件记录
	 * @param fileSetforEmail2
	 *            用户2的全部访问文件记录
	 * @return 共同访问文件数
	 */
	static int calcCommonfile(SortedSet<String> fileSetforEmail1,
			SortedSet<String> fileSetforEmail2) {
		Iterator<String> iterator1 = fileSetforEmail1.iterator();
		Iterator<String> iterator2 = fileSetforEmail2.iterator();

		int count = 0;
		String curString1 = "";
		String curString2 = "";
		int compareResult = 0;// 存储上一轮计算后的状态信息，首轮的时候必然按照两者均取第一位
		while (iterator1.hasNext() && iterator2.hasNext()) {// 如果有一任何一个物品集迭代到了终点，那么后边的物品则无需比较
			switch (compareResult) {
			case 0:// 两者相等，一起向后移位
				curString1 = iterator1.next();
				curString2 = iterator2.next();
				break;
			case 1:// 由于是按照升序存储，因此小的 进行移位
				curString2 = iterator2.next();
				break;
			case -1:
				curString1 = iterator1.next();
			default:
				break;
			}
			compareResult = curString1.compareTo(curString2) == 0 ? 0
					: (curString1.compareTo(curString2) > 0 ? 1 : -1);// 比较两个node的item项大小，小的那个将会向前一位
			if (compareResult == 0) {
				count++;
			}
		}
		while (iterator1.hasNext()) {// 比较最后一个文件是否和后面的还有共同点
			curString1 = iterator1.next();
			if (curString1.equals(curString2)) {
				count++;
				break;
			}
		}
		while (iterator2.hasNext()) {
			curString2 = iterator2.next();
			if (curString1.equals(curString2)) {
				count++;
				break;
			}
		}
		if (debug) {
			System.out.println(fileSetforEmail1 + " " + count);
		}
		return count;
	}

	/**
	 * 计算用户访问过的所有文件
	 * 
	 * @author xiaoluBambi
	 * @param email
	 *            需要查询访问文件的用户
	 * @return email所有文件访问记录
	 * @throws SQLException
	 */
	static SortedSet<String> fileSet(String email, Connection connection)
			throws SQLException {
		Statement statement = connection.createStatement();
		SortedSet<String> resultSet = new TreeSet<String>();
		for (String string : resTableStrings) {// 在全部log记录表中寻找访问记录
			ResultSet resultSet2 = statement
					.executeQuery("select filename FROM " + string
							+ " where email = '" + email + "';");
			while (resultSet2.next()) {
				resultSet.add(resultSet2.getString(1));
			}
		}
		if (debug) {
			for (String string : resultSet) {
				System.out.println(string);
			}
		}
		statement.close();
		return resultSet;
	}

	/**
	 * 将输入参数数据表中
	 * 
	 * @author xiaoluBambi
	 * @throws IOException
	 */
	static void insertIntoTable(String email1, String email2,
			int countofCommonFile, Connection connection) throws IOException {
		String tableForEmail1 = calcHashedTable(email1);// 冗余插入，在一个表中插入e1，e2，在另一个表中相反顺序插入
		String tableForEmail2 = calcHashedTable(email2);
		if (debug) {
			resultBuffer.get(tableForEmail1);
			System.out.println(resultBuffer);
		}
		resultBuffer.get(tableForEmail1).add(
				new nodeEmailCount(email1, email2, countofCommonFile));
		resultBuffer.get(tableForEmail2).add(
				new nodeEmailCount(email2, email1, countofCommonFile));
		// if (resultBuffer.get(tableForEmail1).size() > MAXSIZE) {
		// batchInsert(tableForEmail1, connection);
		// System.out.println(tableForEmail1 + "cleared!");
		// }
		// if (resultBuffer.get(tableForEmail2).size() > MAXSIZE) {
		// batchInsert(tableForEmail2, connection);
		// System.out.println(tableForEmail2 + "cleared!");
		// }
	}

	/**
	 * 批量插入函数
	 * 
	 * @author xiaoluBambi
	 * @param table
	 * @param connection
	 * @return
	 * @throws IOException
	 */
	synchronized static void batchInsert(String table, Connection connection)
			throws IOException {
		try {
			Statement statement = connection.createStatement();
			try {
				connection.setAutoCommit(false);
				Savepoint savepoint = connection.setSavepoint();
				List<String> batchSqlList = new LinkedList<String>();
				try {
					// nodeEmailCount

					for (nodeEmailCount nodeCount : resultBuffer.get(table)) {
						String string = String.format(
								"Insert into %1$s values('%2$s','%3$s',%4$d);",
								table, nodeCount.email, nodeCount.emailRelv,
								nodeCount.commonFileCount);
						batchSqlList.add(string);
						statement.addBatch(string);
					}
					statement.executeBatch();
					connection.commit();
				} catch (SQLException e) {
					// TODO: handle exception
					e.printStackTrace();
//					FileOutputStream fos = new FileOutputStream(
//							"tempresult.out");
//					ObjectOutputStream objectOutputStream = new ObjectOutputStream(
//							fos);
//					objectOutputStream.writeObject(resultBuffer);
//					objectOutputStream.close();
					connection.rollback(savepoint);
					// System.exit(0);
				} finally {
					connection.releaseSavepoint(savepoint);

				}
			} catch (SQLException e) {
				// TODO: handle exception savepoint
//				FileOutputStream fos = new FileOutputStream("tempresult.out");
//				ObjectOutputStream objectOutputStream = new ObjectOutputStream(
//						fos);
//				objectOutputStream.writeObject(resultBuffer);
//				objectOutputStream.close();
				System.err.println("savepoint");
				e.printStackTrace();
			} finally {
				connection.setAutoCommit(true);
			}
		} catch (SQLException e) {
			// TODO: handle exception statement
			System.err.println("statement");
			e.printStackTrace();
		}

		resultBuffer.get(table).clear();
	}

	// @SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {
		// Main main = new Main();

		try {
			Class.forName(driverString).newInstance();
			Connection connection = DriverManager.getConnection(urlString, usr,
					passwd);
			Statement statement = connection.createStatement();
			statement.execute("use test;");
			File file = new File("tempresult2.out");
			// if (file.exists()) {
			// FileInputStream fileInputStream = null;
			// try {
			// fileInputStream = new FileInputStream(file);
			// ObjectInputStream objectInputStream = new ObjectInputStream(
			// fileInputStream);
			// resultBuffer = (Map<String, Queue<nodeEmailCount>>)
			// objectInputStream
			// .readObject();
			// for (int i = 0; i < tableStrings.length; i++) {
			// batchInsert(tableStrings[i], connection);
			// }
			// } catch (Exception e) {
			// // TODO: handle exception
			// } finally {
			// file.delete();
			// }
			// }

			List<String> emailList = new ArrayList<String>();
			ResultSet resultSet = statement
					.executeQuery("select email from userset where count > 10;");
			while (resultSet.next()) {
				emailList.add(resultSet.getString(1));
			}

			System.out.println("start!");
			System.out.println("total users:" + emailList.size());

			// int nextFinishedLine = 16130000;// 没有用处，记录一下上次完成的行数
			int countSql = 0;// 执行过的select语句数
			int maxSizeforBufferMap = 3000;// 缓冲区最大容量
			int offset = 3744;// 首次循环偏移量
			int startLine = 4271;// 开始行数
			long finshedNumber = 34885000;// 当前完成行数,实际完成行数需要减掉start
			int step = 5000;// 每次插入间隔行数
			Map<String, SortedSet<String>> bufferMap = new ConcurrentHashMap<String, SortedSet<String>>();

			for (int i = 0; i < tableStrings.length; i++) {
				batchInsert(tableStrings[i], connection);
			}
			for (int i = startLine; i < emailList.size() - 1; i++) {
				SortedSet<String> sortedSet1 = null;
				if (!bufferMap.containsKey(emailList.get(i))) {
					sortedSet1 = fileSet(emailList.get(i), connection);
					countSql++;
					bufferMap.put(emailList.get(i), sortedSet1);
				} else {
					sortedSet1 = bufferMap.get(emailList.get(i));

				}
				for (int j = i + offset; j < emailList.size(); j++) {
					SortedSet<String> sortedSet2 = null;
					if (!bufferMap.containsKey(emailList.get(j))) {
						sortedSet2 = fileSet(emailList.get(j), connection);
						countSql++;
						if (bufferMap.size() < maxSizeforBufferMap) {
							bufferMap.put(emailList.get(j), sortedSet2);
						}
					} else {
						sortedSet2 = bufferMap.get(emailList.get(j));

					}
					int count = calcCommonfile(sortedSet1, sortedSet2);
					if (count > THREHOLD) {
						insertIntoTable(emailList.get(i), emailList.get(j),
								count, connection);
						if (debug) {
							System.out.println(emailList.get(i) + " "
									+ emailList.get(j) + " " + count);
						}
					}
					finshedNumber++;
					if (finshedNumber % step == 0) {
						// throw(new Exception());
						for (int k = 0; k < tableStrings.length; k++) {
							batchInsert(tableStrings[k], connection);
						}
						System.out.println(finshedNumber);
						FileWriter fileWriter = new FileWriter(file);
						PrintWriter printWriter = new PrintWriter(fileWriter);
						printWriter.println(finshedNumber);
						printWriter.flush();
					}
				}
				bufferMap.remove(emailList.get(i));
				if (debug) {
					System.out.println(emailList.get(i));
				}
				offset = 1;
				System.out.println(countSql);
				System.out.println(bufferMap.size());
			}

			for (int i = 0; i < tableStrings.length; i++) {
				batchInsert(tableStrings[i], connection);
			}
		} catch (Exception e) {
			// TODO: handle exception
			FileOutputStream fos = new FileOutputStream("tempresult.out");
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fos);
			objectOutputStream.writeObject(resultBuffer);
			objectOutputStream.close();
			System.err.println("savepoint");
			e.printStackTrace();
		}

	}
}
