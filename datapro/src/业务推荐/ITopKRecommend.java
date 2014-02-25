package ҵ���Ƽ�;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface ITopKRecommend {

	public void testProxy(Connection connection) throws SQLException;

	public Map<String, Double> getAllUser(Connection connection);

	public Map<String, Double> getAllRelevanceUser(String user,
			Connection connection);

	public Map<String, Double> getRelevanceUsers(String user,
			Connection connection);
	/**
	 * �ܹ�ʹ��һ�������ļ��������в��� ʵ����һ�����ص�Cf�㷨 ֻʹ�����û������
	 * 
	 * @throws SQLException
	 */
	public int[] topNforProOne(int topN, int V, int Nhead, int maxIterNum)
			throws IOException, SQLException;

	/**
	 * topKUsers
	 */
	public Set<String> topKUsers(int Nhead, Map<String, Double> input);

	/**
	 * V��ʼ��������һ��ѵ�������е�ȫ���û�����Ϊÿ���û���ʼ�����ʸ��� visitUsers�������ȡV����������V����������ɾ�����������������
	 * ����V�д��ڣ���1 ����V�в����ڵģ���0
	 * 
	 * @param visitUsers
	 *            :���ݼ���ȫ�����ʣ�ִ�к�ʣ��Ĳ�����Ϊ����
	 * @param V
	 *            :��Ҫ�ռ���ҵ�������
	 * @param trainSet
	 *            :�Ѿ��ռ����ķ��ʼ�¼��ͨ���ú����������
	 */
	public Map<String, Double> initialV(int V, Set<String> visitUsers,
			Set<String> trainSet);

	/**
	 * ��һ�ε������½ڵ��ֵ
	 * */
	public void iterOnce(Map<String, Double> input, Set<String> trainSet);

	/**
	 * ���ĳ��ҵ���ȫ������ ��ȡ�������� ��������δ��ɣ���δ����ɸѡ���ܣ�Ӧ��ѵ�����ݼ�����δ���ֵ��û�ɾ��
	 */
	public Set<String> getAllVisit(String trafficString, Connection connection)
			throws SQLException;

	public Connection makeConnection() throws SQLException;

	/* ???????????????????????????????? */
	public Connection getConnection();

	public void releaseConnection(Connection connection);

	/**
	 * ��ȡ�������ݼ����е�ȫ������ ��ȡ��������
	 */
	public Set<String> getAllTraffic(int minCount, int maxCount, Connection connection);
}
