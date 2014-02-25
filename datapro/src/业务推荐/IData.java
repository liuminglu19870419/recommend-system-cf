package 业务推荐;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface IData extends Serializable{

	/**
	 * 动态代理测试
	 * */
	public void testProxy(Connection connection) throws SQLException;

	/**
	 * @return 数据集全部用用户及其访问文件总数
	 */
	public Map<String, Double> getAllUser(Connection connection);

	/**
	 * @return 获取测试数据集合中的全部数据 读取测试数据
	 */
	public Set<String> getAllTraffic(Connection connection);
	/**
	 * @return 获取user的全部满足条件的相关用户及其相关系数
	 */
	public Map<String, Double> getAllRelevanceUser(String user,int topK,
			Connection connection);

	/**
	 * 获得某个业务的全部访问 读取测试数据 ？？？？未完成，尚未加入筛选功能，应将训练数据集合中未出现的用户删除
	 */
	public Set<String> getAllVisit(String trafficString, Connection connection)
			throws SQLException;

	/**
	 * 生成一个链接
	 * */
	public Connection makeConnection() throws SQLException;

	/**
	 * 从链接池获取一个链接
	 * */
	public Connection getConnection();

	/**
	 * 释放从链接池获取的链接
	 * */
	public void releaseConnection(Connection connection);

}
