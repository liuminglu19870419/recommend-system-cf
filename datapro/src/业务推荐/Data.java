package ҵ���Ƽ�;

import java.awt.print.Printable;
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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

//Ȼ�����ÿһ�����ܺ���
/**
 * ���ݼ���
 * */
public class Data implements IData {

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
	private Map<String, Double> userCountMap = null;// �û������ļ������棨��Ϊ��С���ҳ�����˻��棩
	private Set<String> fileCountMap = null;

	/*********************** �㷨���� ***************************/
	private double ALPHA = 0.4;// �û���ϵ������ֵ�������ϵ�����������ֵ���û����ᱻ���뵽������
	private int FILEMIN = 900, FILEMAX = 1000;// �����ļ�ȡֵ��Χ��������Ҫ������ƣ�ͨ��ѡȡ����һ���������ļ���Ϊ��������
	private int USERMIN = 10, USERMAX = 100000000;
	/* ���ݿ����ӳ� */
	private LinkedBlockingDeque<Connection> connectionPool = null;
	private int MAXCONNECTION = 20;

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
		releaseConnection(connection);
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
	public Map<String, Double> getAllUser(Connection connection) {
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
					userCountMap.put(resultSet.getString(1),
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
	public Map<String, Double> getAllRelevanceUser(String user, int topK,
			Connection connection) {
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
			resultMap.put(entries.get(i).getKey(),
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
		return connectionPool.removeFirst();
	}

	public void releaseConnection(Connection connection) {
		if (connection == null)
			return;
		connectionPool.addLast(connection);
	}

	/**
	 * ��ȡ�������ݼ����е�ȫ������ ��ȡ�������� ��δ����ҵ���������ȡ?????????????????????
	 */
	public synchronized Set<String> getAllTraffic(Connection connection) {
		if (fileCountMap == null) {
			connection = getConnection();
			Statement statement = null;
			Set<String> hashSet = null;
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
		ConnectionProxy connectionProxy = new ConnectionProxy();
		IData topKRecommend = (IData) connectionProxy.bind(new Data());
		topKRecommend.testProxy(null);
		Set<String > allTrafficSet = topKRecommend.getAllTraffic(null);
		Map<String, Double> alluesrMap = topKRecommend.getAllUser(null);
		Set<String> allVisitSet = topKRecommend.getAllVisit("929faf93c9bc85b80d1fae73cf44790b", null);	
		Map<String, Double> map = topKRecommend.getAllRelevanceUser("76c4be95763805c3aa3de80918a4b4f0", 150, null);
	}
}
