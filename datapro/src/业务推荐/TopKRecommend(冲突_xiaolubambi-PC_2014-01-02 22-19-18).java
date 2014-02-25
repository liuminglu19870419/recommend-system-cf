package ҵ���Ƽ�;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

//�д��ڽ�������� ����Ҫ���㷨���ֵ����ݳ������Ȼ�����ÿһ�����ܺ���
public class TopKRecommend implements Runnable {

	public TopKRecommend(int topK, int V, int Nhead[], int maxIterNum,
			IData data) {
		// TODO Auto-generated constructor stub
		this.topK = topK;
		this.V = V;
		this.Nhead = Nhead;
		this.data = data;
	}

	static private String filename = null;
	private int topK;
	private int V;
	private int Nhead[];
	private int maxIterNum;
	private IData data;

	static {
		String systemPathString = Thread.currentThread()
				.getContextClassLoader().getResource("").getPath();
		Date curDate = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yy-MM-dd-hh-mm-ss");
		filename = systemPathString + "TKNIterator"
				+ dateFormat.format(curDate) + ".txt";
	}

	/**
	 * �ܹ�ʹ��һ�������ļ��������в��� ʵ����һ�����ص�Cf�㷨 ֻʹ�����û������
	 * ������ݿ��Խ����Ż������Ը���topN��Nhead��ѡȡ�����Ż���ʹ�ø��ٵļ����Ϊ����
	 * 
	 * @throws SQLException
	 */
	public void topNforProOne(int topK, int V, int Nhead[], int maxIterNum)
			throws IOException, SQLException {

		System.out
				.println("V:" + V + " N:" + Nhead + "maxIterNum" + maxIterNum);

		// �����㷨����ȡ��������

		Set<String> testTraffic = data.getAllTraffic(null);
		// System.out.println("V:" + V + " N:" + Nhead + "iterNum" + iterNum
		// + "all traffic");
		int[][] rightPrePushed = new int[maxIterNum][Nhead.length];// ���������淽ʽ�����飿������
		int[][] totalPrePushed = new int[maxIterNum][Nhead.length];
		int[][] totalAccessed = new int[maxIterNum][Nhead.length];
		for (String string : testTraffic) {
			// ��ȡ����ҵ���ȫ��ҵ�����
			Set<String> visitUsers = data.getAllVisit(string, null);

			Set<String> trainSet = new HashSet<String>();
			Map<String, Double> input = initialV(V, visitUsers, trainSet);

			for (int j = 0; j < maxIterNum; j++) {
				iterOnce(input, trainSet, topK);
				// Ӧ���ƶ�������ָ��ҵ����е����Ƽ�
				List<String> userSortedByRate = topKUsers(input);
				for (int i = 0; i < Nhead.length; i++) {// ����NheadѡȡǰNhead���û���Ϊ�Ƽ��û�
					int userCount = userSortedByRate.size() > Nhead[i] ? Nhead[i]
							: userSortedByRate.size();
					totalAccessed[j][i] += visitUsers.size();
					totalPrePushed[j][i] += userCount;
					for (int k = 0; k < userCount; k++) {
						if (visitUsers.contains(userSortedByRate.get(k))) {
							rightPrePushed[j][i]++;
						}
					}
				}
			}
		}

		// �洢���//
		synchronized (filename) {
			PrintWriter resultPrintStream = new PrintWriter(new FileWriter(
					filename, true));
			for (int j = 0; j < maxIterNum; j++) {
				for (int i = 0; i < Nhead.length; i++) {
					resultPrintStream
							.println(topK + "," + V + "," + Nhead[i] + "," + j
									+ 1 + "," + rightPrePushed[j][i] + ","
									+ totalPrePushed[j][i] + ","
									+ +totalAccessed[j][i]);
				}
			}

			resultPrintStream.flush();
			resultPrintStream.close();
		}

		System.out.println("finished!!");

	}

	/**
	 * topKUsers
	 */
	public List<String> topKUsers(Map<String, Double> input) {
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
		List<String> resultList = new LinkedList<String>();
		for (int i = 0; i < entries.size(); i++) {
			resultList.add(entries.get(i).getKey());
		}

		return resultList;
	}

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
			Set<String> trainSet) {
		Map<String, Double> resultMap = new HashMap<String, Double>();

		for (String string : data.getAllUser(null).keySet()) {
			resultMap.put(string, .0);
		}

		String[] tempVisitUserSet = new String[visitUsers.size()];
		visitUsers.toArray(tempVisitUserSet);

		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < V; i++) {
			if (tempVisitUserSet.length - i - 1 <= 0) {
				System.out.println(tempVisitUserSet.length + " " + i);
				System.exit(0);
			}
			int offset = random.nextInt(tempVisitUserSet.length - i - 1);

			String tempString = tempVisitUserSet[i];
			tempVisitUserSet[i] = tempVisitUserSet[i + offset];
			tempVisitUserSet[i + offset] = tempString;

			if (resultMap.containsKey(tempVisitUserSet[i])) {
				resultMap.remove(tempVisitUserSet[i]);// �����������������Ѿ����۵�ҵ�����
				trainSet.add(tempVisitUserSet[i]);// �Ѿ���ȡ��ҵ�����
				visitUsers.remove(tempVisitUserSet[i]);// ԭ��¼����ʣ��Ĳ�����Ϣ
			}
		}

		return resultMap;
	}

	/**
	 * ��һ�ε������½ڵ��ֵ
	 * */
	public void iterOnce(Map<String, Double> input, Set<String> trainSet,
			int topN) {
		for (String string : input.keySet()) {

			double newValue = 0;
			Map<String, Double> relatUseRate = data.getAllRelevanceUser(string,
					topN, null);
			for (String string2 : relatUseRate.keySet()) {

				if (input.keySet().contains(string2)) {
					newValue += relatUseRate.get(string2) * input.get(string2);
				} else if (trainSet.contains(string2)) {
					newValue += relatUseRate.get(string2) * 1.0;
				}

			}
			input.put(string, newValue);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			topNforProOne(topK, V, Nhead, maxIterNum);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		int topK[] = { 150 };
		int V[] = new int[22];
		for (int i = 0; i < 15; i++) {
			V[i] = 1 + i * 1;
		}
		for (int i = 15; i <= 21; i++) {
			V[i] = (i - 15) * 5 + 20;
		}
		for (int i = 0; i < V.length; i++) {
			System.out.println(V[i]);
		}
		int Nhead[] = new int[10];
		for (int i = 0; i < Nhead.length; i++) {
			Nhead[i] = 10 + i * 10;
		}

		int iterNum[] = new int[10];
		for (int i = 0; i < iterNum.length; i++) {
			iterNum[i] = i + 1;
		}
		int maxIterNum = 10;

		IData data = null;
		ConnectionProxy connectionProxy = new ConnectionProxy();
		data = (IData) connectionProxy.bind(new Data());
		
		ExecutorService executorService = Executors.newFixedThreadPool(22);
		Date curDate = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yy-MM-dd-hh-mm-ss");
		System.out.println("calc started!" + dateFormat.format(curDate));
		for (int i = 0; i < iterNum.length; i++) {
			executorService.execute(new TopKRecommend(topK[0], V[i], Nhead,
					maxIterNum, data));
		}
		// �ر������߳�
		executorService.shutdown();
		// �ȴ����߳̽������ټ���ִ������Ĵ���
		try {
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dateFormat = new SimpleDateFormat("yy-MM-dd-hh-mm-ss");
		System.out.println("calc ended!" + dateFormat.format(curDate));

	}
}
