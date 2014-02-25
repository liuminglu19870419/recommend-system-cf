package �����û���ϵ����;

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

	static final boolean debug = false;// �����Ƿ����debug��ʱ��Ϣ
	static final String[] tableStrings = { "userrel1", "userrel2", "userrel3",
			"userrel4", "userrel5", "userrel6", "userrel7", "userrel8",
			"userrel9", "userrel10" };// �û���ϵ��
	static final String[] resTableStrings = { "teif_1", "teif_2", "teif_3" };// ԭ���ķ���log
	static Map<String, Queue<nodeEmailCount>> resultBuffer;// �洢��ʱ��Ϣ�Ļ����¼��
	static final int MAXSIZE = 1024;
	static final int THREHOLD = 10;// ��ͬ�ļ���ֵ����

	static final String urlString = "jdbc:mysql://" + "166.111.5.193:5030";// ���ݿ���������
	static final String usr = "meepo";// �û���
	static final String passwd = "meepo";// ����
	static final String driverString = "com.mysql.jdbc.Driver";// ������������
	static final String database = "test";

	// ��ʱ��ϵ��
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
	 * ʹ��hashֵ�þ���ֵ����email�����ı�
	 * 
	 * @author xiaoluBambi
	 * @param email
	 * @return ���û������ı�
	 */
	static String calcHashedTable(String email) {
		return tableStrings[Math.abs(email.hashCode()) % tableStrings.length];
	}

	/**
	 * ���㹲ͬ�����ļ���
	 * 
	 * @author xiaoluBambi
	 * @param fileSetforEmail1
	 *            �û�1��ȫ�������ļ���¼
	 * @param fileSetforEmail2
	 *            �û�2��ȫ�������ļ���¼
	 * @return ��ͬ�����ļ���
	 */
	static int calcCommonfile(SortedSet<String> fileSetforEmail1,
			SortedSet<String> fileSetforEmail2) {
		Iterator<String> iterator1 = fileSetforEmail1.iterator();
		Iterator<String> iterator2 = fileSetforEmail2.iterator();

		int count = 0;
		String curString1 = "";
		String curString2 = "";
		int compareResult = 0;// �洢��һ�ּ�����״̬��Ϣ�����ֵ�ʱ���Ȼ�������߾�ȡ��һλ
		while (iterator1.hasNext() && iterator2.hasNext()) {// �����һ�κ�һ����Ʒ�����������յ㣬��ô��ߵ���Ʒ������Ƚ�
			switch (compareResult) {
			case 0:// ������ȣ�һ�������λ
				curString1 = iterator1.next();
				curString2 = iterator2.next();
				break;
			case 1:// �����ǰ�������洢�����С�� ������λ
				curString2 = iterator2.next();
				break;
			case -1:
				curString1 = iterator1.next();
			default:
				break;
			}
			compareResult = curString1.compareTo(curString2) == 0 ? 0
					: (curString1.compareTo(curString2) > 0 ? 1 : -1);// �Ƚ�����node��item���С��С���Ǹ�������ǰһλ
			if (compareResult == 0) {
				count++;
			}
		}
		while (iterator1.hasNext()) {// �Ƚ����һ���ļ��Ƿ�ͺ���Ļ��й�ͬ��
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
	 * �����û����ʹ��������ļ�
	 * 
	 * @author xiaoluBambi
	 * @param email
	 *            ��Ҫ��ѯ�����ļ����û�
	 * @return email�����ļ����ʼ�¼
	 * @throws SQLException
	 */
	static SortedSet<String> fileSet(String email, Connection connection)
			throws SQLException {
		Statement statement = connection.createStatement();
		SortedSet<String> resultSet = new TreeSet<String>();
		for (String string : resTableStrings) {// ��ȫ��log��¼����Ѱ�ҷ��ʼ�¼
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
	 * ������������ݱ���
	 * 
	 * @author xiaoluBambi
	 * @throws IOException
	 */
	static void insertIntoTable(String email1, String email2,
			int countofCommonFile, Connection connection) throws IOException {
		String tableForEmail1 = calcHashedTable(email1);// ������룬��һ�����в���e1��e2������һ�������෴˳�����
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
	 * �������뺯��
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

			// int nextFinishedLine = 16130000;// û���ô�����¼һ���ϴ���ɵ�����
			int countSql = 0;// ִ�й���select�����
			int maxSizeforBufferMap = 3000;// �������������
			int offset = 3744;// �״�ѭ��ƫ����
			int startLine = 4271;// ��ʼ����
			long finshedNumber = 34885000;// ��ǰ�������,ʵ�����������Ҫ����start
			int step = 5000;// ÿ�β���������
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
