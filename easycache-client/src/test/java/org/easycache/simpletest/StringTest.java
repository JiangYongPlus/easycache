package org.easycache.simpletest;

import org.easycache.utility.Print;
import java.util.Stack;

public class StringTest {
	private static String sql = "  select * from table ";

	public static void trim(String sql) {
		Print.show(sql.trim());
	}

	public static void main(String[] args) {
		trim(sql);
	}

	
	public int minDistance(String word1, String word2) {
		if (word1 == null && word2 == null)
			return 0;
		if (word1 == null)
			return word2.length();
		if (word2 == null)
			return word1.length();
		int m = word1.length() + 1;
		int n = word2.length() + 1;
		int[][] distance = new int[m][n];
		for (int i = 0; i < m; i++) {
			distance[i][0] = i;
		}
		for (int j = 1; j < n; j++) {
			distance[0][j] = j;
		}
		for (int i = 1; i < m; i++) {
			for (int j = 1; j < n; j++) {
				distance[i][j] = min(
						distance[i][j - 1] + 1,
						distance[i - 1][j] + 1,
						distance[i - 1][j - 1]
								+ (word1.charAt(i - 1) == word2.charAt(j - 1) ? 0
										: 1));
			}
		}
		return distance[m - 1][n - 1];
	}

	public int min(int x, int y, int z) {
		return (x = x < y ? x : y) < z ? x : z;
	}
	
	public String simplifyPath(String path) {
		if (path == null || path.length() == 0)
			return "/";
		String[] splits = path.split("/");
		Stack<String> stack = new Stack<String>();
		for (int i = 0; i < splits.length; i++) {
			if (splits[i] == "" || splits[i] == ".") {
				continue;
			} else if (splits[i] == ".." && !stack.isEmpty()) {
				stack.pop();
			} else {
				stack.push(splits[i]);
			}
		}
		String spath = "";
		if (stack.size() == 0)
			return "/";
		for (int i = 0; i < stack.size(); i++) {
			spath = spath.concat("/".concat(stack.get(i)));
		}
		return spath;
	}
	
}
