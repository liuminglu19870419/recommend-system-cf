package 蛋疼的去掉index;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

	public static void main(String[] args) throws IOException {
		File file = new File("J:\\数据备份\\新数据去掉重复访问.sql");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		File file2 = new File("J:\\数据备份\\新数据去掉重复访问2.sql");
		FileWriter fileWriter = new FileWriter(file2);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		String string = bufferedReader.readLine();
		int count = 0;
		while ( string != null) {
			System.out.println(string);
			if (string.contains("INSERT")) {
				bufferedWriter.write(string);
				string = bufferedReader.readLine();
				break;
			}
			string = bufferedReader.readLine();
		}
		while (string != null) {
			bufferedWriter.write(string);
			string = bufferedReader.readLine();
			count ++;
			if (count % 10000 == 0) {
				System.out.println(count);
			}
		}
	}
}
