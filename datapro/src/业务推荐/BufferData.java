package ҵ���Ƽ�;

import java.awt.RadialGradientPaint;
import java.awt.print.Printable;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//Ȼ�����ÿһ�����ܺ���
/**
 * ���ݼ���
 * */
public class BufferData implements IData, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/* �������ݿ����������Ϣ */
	private String usr = "meepo";// �û���
	private String passwd = "meepo";// ����
	private String driverString = "com.mysql.jdbc.Driver";// ������������
	private String database = "test";// ���ݿ�
	private String urlString = "jdbc:mysql://" + "166.111.5.193:5030";// MySql��URL

	/* ��������Ϣ */
	private String[] userRelationRateTables = { "userrel1", "userrel2",
			"userrel3", "userrel4", "userrel5", "userrel6", "userrel7",
			"userrel8", "userrel9", "userrel10" };// �û���ϵ�����
	private String fileSetTable = "fileset";// ȫ���ļ�����
	private String userSetTable = "userset";// ȫ���û�����
	private String[] logRecordTables = { "teif_1", "teif_2", "teif_3" };// �����������log���ݼ�
	private HashMap<String, Double> userCountMap = null;// �û������ļ������棨��Ϊ��С���ҳ�����˻��棩
	private HashSet<String> fileCountMap = null;

	/*********************** �㷨���� ***************************/
	private double ALPHA = 0.004;// �û���ϵ������ֵ�������ϵ�����������ֵ���û����ᱻ���뵽������
	private int FILEMIN = 200, FILEMAX = 300;// �����ļ�ȡֵ��Χ��������Ҫ������ƣ�ͨ��ѡȡ����һ���������ļ���Ϊ��������
	private int USERMIN = 10, USERMAX = 100000000;
	/* ���ݿ����ӳ� */
	transient private LinkedBlockingDeque<Connection> connectionPool = null;
	private int MAXCONNECTION = 50;

	// static class Node {
	// String userString;
	//
	// Map<String, Double> relevanceUserMap = null;
	//
	// public Node(){
	// ;
	// }
	// public Node(String userString) {
	// this.userString = userString;
	// }
	// @Override
	// public boolean equals(Object object) {
	// return ((Node) object).userString.equals(userString);
	// }
	//
	// @Override
	// public int hashCode() {
	// return userString.hashCode();
	// }
	// }
	//
	// private List<Node> bufferNodes = null;
	private HashMap<String, HashMap<String, Double>> bufferMap = null;

	// �����飺�������ݿ���������ʼ�����ӳأ���ʼ���û������ļ�������
	{
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
		connectionPool = new LinkedBlockingDeque<Connection>();
		for (int i = 0; i < MAXCONNECTION; i++) {
			try {
				connectionPool.add(makeConnection());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Connection connection = getConnection();
		userCountMap = getAllUser(connection);
		fileCountMap = getAllTraffic(connection);
		// bufferNodes = new ArrayList<BufferData.Node>();
		bufferMap = new HashMap<String, HashMap<String, Double>>();

		int count = 0;
		for (String user : userCountMap.keySet()) {
			Map<String, Double> relvanceUserMap = getAllRelevanceUser(user,
					500, connection);
			bufferMap.put(user.intern(), new HashMap<String, Double>());

			for (String relavUser : relvanceUserMap.keySet()) {
				bufferMap.get(user).put(relavUser.intern(),
						relvanceUserMap.get(relavUser));
			}

			count++;
			if (count % 1000 == 0)
				System.out.println("load finished:" + count);
		}
		releaseConnection(connection);
	}

	public void init() {
		if (connectionPool == null) {
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
			connectionPool = new LinkedBlockingDeque<Connection>();
			for (int i = 0; i < MAXCONNECTION; i++) {
				try {
					connectionPool.add(makeConnection());
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * �ͷ�ȫ������
	 */
	@Override
	public void finalize() {
		Iterator<Connection> iterator = connectionPool.iterator();
		while (iterator.hasNext()) {
			try {
				iterator.next().close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * ���Դ���
	 */
	public void testProxy(Connection connection) throws SQLException {
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement
				.executeQuery("select * from `teif_1` limit 1000");
		while (resultSet.next()) {
			System.out.println(resultSet.getString(1));
		}
	}

	/**
	 * ��ȡ���ݼ�����ȫ���û�
	 */
	public HashMap<String, Double> getAllUser(Connection connection) {
		if (userCountMap == null) {
			Statement statement = null;
			try {
				userCountMap = new HashMap<String, Double>();
				statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("select * from `"
						+ userSetTable + "` where `count` >= "
						+ Integer.toString(USERMIN) + " and `count` <= "
						+ Integer.toString(USERMAX) + ";");
				while (resultSet.next()) {
					userCountMap.put(resultSet.getString(1).intern(),
							resultSet.getDouble(2));
				}
				return userCountMap;
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					statement.close();
				} catch (SQLException e2) {
					// TODO: handle exception
					e2.printStackTrace();
				}

			}
		} else {
			return userCountMap;
		}

		return null;
	}

	/**
	 * ��ȡȫ������û�
	 */
	volatile private int preTopK = 1;
	transient private Map<String, Map<String, Double>> topKBufferMap = new ConcurrentHashMap<String, Map<String, Double>>();
	private AtomicInteger preTopKAtomicInteger = new AtomicInteger();

	private Lock readLock = new ReentrantLock();

	public Map<String, Double> getAllRelevanceUser(String user, int topK,
			Connection connection) {
		if (bufferMap.get(user) != null) {

			try {
				readLock.lock();
				if (topK != preTopK) {
			
						topKBufferMap = new HashMap<String, Map<String, Double>>();
						preTopK = topK;

				}
				if (topKBufferMap.get(user) == null) {

						topKBufferMap.put(user,
								topKUsers(preTopK, bufferMap.get(user)));
						double totalRelvRate = 0;
						for (String tempUserString : topKBufferMap.get(user).keySet()) {
							totalRelvRate += topKBufferMap.get(user).get(tempUserString);
						}
						for (String tempUserString : topKBufferMap.get(user).keySet()) {
							Double oldValueDouble = topKBufferMap.get(user).get(tempUserString);
							topKBufferMap.get(user).put(tempUserString, oldValueDouble / totalRelvRate);
						}

				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				readLock.unlock();
			}

			return topKBufferMap.get(user);

		}
		// return topKUsers(topK, bufferMap.get(user));
		// return bufferMap.get(user);
		int userNum = Math.abs(user.hashCode()) % userRelationRateTables.length;// �����û�ID��hashֵ�������������ݼ�
		String tableString = userRelationRateTables[userNum];
		Map<String, Double> relevanceUserMap = null;

		Statement statement = null;
		try {
			relevanceUserMap = new HashMap<String, Double>();
			statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("select * from `"
					+ tableString + "` where `email` = '" + user + "';");
			while (resultSet.next()) {

				String relUserString = resultSet.getString(2);
				if (userCountMap.keySet().contains(relUserString)) {
					double relRate = resultSet.getInt(3)
							/ userCountMap.get(relUserString);
					if (relRate > ALPHA) {
						relevanceUserMap.put(relUserString, relRate);
					}
				}
			}
			relevanceUserMap = topKUsers(topK, relevanceUserMap);
			return relevanceUserMap;
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				statement.close();
			} catch (SQLException e2) {
				// TODO: handle exception
			}

		}
		return null;
	}

	public Map<String, Double> topKUsers(int Nhead, Map<String, Double> input) {
		Set<Entry<String, Double>> entrySet = input.entrySet();
		ArrayList<Entry<String, Double>> entries = new ArrayList<Map.Entry<String, Double>>(
				entrySet);
		Collections.sort(entries, new Comparator<Entry<String, Double>>() {

			@Override
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				// TODO Auto-generated method stub
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		int MaxNPrepushed = input.size() > Nhead ? Nhead : input.size();// ����Ƽ��û���
		Map<String, Double> resultMap = new HashMap<String, Double>();
		for (int i = 0; i < MaxNPrepushed; i++) {
			resultMap.put(entries.get(i).getKey().intern(),
					input.get(entries.get(i).getKey()));
		}

		return resultMap;
	}

	/**
	 * ���ĳ��ҵ���ȫ������ ��ȡ��������δ��ɣ���δ����ɸѡ���ܣ�Ӧ��ѵ�����ݼ�����δ���ֵ��û�ɾ��
	 */
	public synchronized Set<String> getAllVisit(String trafficString,
			Connection connection) throws SQLException {

		Statement statement = null;
		SortedSet<String> resultStrings = new TreeSet<String>();
		try {
			statement = connection.createStatement();
			ResultSet resultSet = null;
			for (int i = 0; i < logRecordTables.length; i++) {
				resultSet = statement.executeQuery("select `email` from `"
						+ logRecordTables[i] + "` where `filename` ='"
						+ trafficString + "';");
				while (resultSet.next()) {
					// System.out.println(resultSet.getString(1));
					if (userCountMap.keySet().contains(resultSet.getString(1))) {
						resultStrings.add(resultSet.getString(1));
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return resultStrings;
	}

	public Connection makeConnection() throws SQLException {
		Connection connection = DriverManager.getConnection(urlString, usr,
				passwd);
		Statement statement = connection.createStatement();
		statement.execute("use " + database + ";");
		return connection;
	}

	/*  */
	public Connection getConnection() {
		try {
			return connectionPool.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void releaseConnection(Connection connection) {
		if (connection == null)
			return;
		connectionPool.addLast(connection);
	}

	/**
	 * ��ȡ�������ݼ����е�ȫ������ ��ȡ�������� ��δ����ҵ���������ȡ?????????????????????
	 */
	public synchronized HashSet<String> getAllTraffic(Connection connection) {
		if (fileCountMap == null) {
			Statement statement = null;
			HashSet<String> hashSet = null;
			try {
				statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("select * from `"
						+ fileSetTable + "` where `count` >= "
						+ Integer.toString(FILEMIN) + " and `count` <= "
						+ Integer.toString(FILEMAX) + ";");
				hashSet = new HashSet<String>();
				while (resultSet.next()) {
					hashSet.add(resultSet.getString(1));
				}
				return hashSet;
			} catch (SQLException exception) {
				exception.printStackTrace();
			} finally {
				try {
					statement.close();
				} catch (SQLException e) {
					// TODO: handle exception
					e.printStackTrace();
				} finally {

				}

			}
		} else {
			return fileCountMap;
		}
		return null;
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws SQLException {
		BufferData bufferData = new BufferData();
		Connection connection = bufferData.getConnection();
		Set<String> allTrafficSet = bufferData.getAllTraffic(connection);
		Map<String, Double> alluesrMap = bufferData.getAllUser(connection);
		Set<String> allVisitSet = bufferData.getAllVisit(
				"929faf93c9bc85b80d1fae73cf44790b", connection);
		Map<String, Double> map = bufferData.getAllRelevanceUser(
				"76c4be95763805c3aa3de80918a4b4f0", 150, connection);
		bufferData.releaseConnection(connection);
	}
}
