package 业务推荐;

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
	 * 能够使用一个配置文件配置所有测试 实现了一个朴素的Cf算法 只使用了用户相关性
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
	 * V初始化，生成一个训练集合中的全部用户，并为每个用户初始化访问概率 visitUsers中随机抽取V个，并将这V个从输入中删除，用以最后做测试
	 * 对于V中存在，附1 对于V中不存在的，附0
	 * 
	 * @param visitUsers
	 *            :数据集中全部访问，执行后剩余的部分作为测试
	 * @param V
	 *            :需要收集的业务访问数
	 * @param trainSet
	 *            :已经收集到的访问记录，通过该函数随机生成
	 */
	public Map<String, Double> initialV(int V, Set<String> visitUsers,
			Set<String> trainSet);

	/**
	 * 做一次迭代更新节点得值
	 * */
	public void iterOnce(Map<String, Double> input, Set<String> trainSet);

	/**
	 * 获得某个业务的全部访问 读取测试数据 ？？？？未完成，尚未加入筛选功能，应将训练数据集合中未出现的用户删除
	 */
	public Set<String> getAllVisit(String trafficString, Connection connection)
			throws SQLException;

	public Connection makeConnection() throws SQLException;

	/* ???????????????????????????????? */
	public Connection getConnection();

	public void releaseConnection(Connection connection);

	/**
	 * 获取测试数据集合中的全部数据 读取测试数据
	 */
	public Set<String> getAllTraffic(int minCount, int maxCount, Connection connection);
}
