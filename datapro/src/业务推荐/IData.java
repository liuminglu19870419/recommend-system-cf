package ҵ���Ƽ�;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface IData extends Serializable{

	/**
	 * ��̬�������
	 * */
	public void testProxy(Connection connection) throws SQLException;

	/**
	 * @return ���ݼ�ȫ�����û���������ļ�����
	 */
	public Map<String, Double> getAllUser(Connection connection);

	/**
	 * @return ��ȡ�������ݼ����е�ȫ������ ��ȡ��������
	 */
	public Set<String> getAllTraffic(Connection connection);
	/**
	 * @return ��ȡuser��ȫ����������������û��������ϵ��
	 */
	public Map<String, Double> getAllRelevanceUser(String user,int topK,
			Connection connection);

	/**
	 * ���ĳ��ҵ���ȫ������ ��ȡ�������� ��������δ��ɣ���δ����ɸѡ���ܣ�Ӧ��ѵ�����ݼ�����δ���ֵ��û�ɾ��
	 */
	public Set<String> getAllVisit(String trafficString, Connection connection)
			throws SQLException;

	/**
	 * ����һ������
	 * */
	public Connection makeConnection() throws SQLException;

	/**
	 * �����ӳػ�ȡһ������
	 * */
	public Connection getConnection();

	/**
	 * �ͷŴ����ӳػ�ȡ������
	 * */
	public void releaseConnection(Connection connection);

}
