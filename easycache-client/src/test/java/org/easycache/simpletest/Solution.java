package org.easycache.simpletest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

public class Solution {
	public boolean isValid(String s) {
		if (s == null || s.length() == 0)
			return true;
		Stack<Character> stack = new Stack<Character>();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '(' || c == '[' || c == '{') {
				stack.push(c);
			} else if (c == ')' || c == ']' || c == '}') {
				if (!stack.isEmpty()) {
					if (!match(stack.pop(), c)) {
						return false;
					}
				}
				return false;
			} else {
				return false;
			}
		}
		if (stack.isEmpty()) {
			return true;
		}
		return false;
	}

	boolean match(char left, char right) {
		if (left == '(' && right == ')')
			return true;
		if (left == '[' && right == ']')
			return true;
		if (left == '{' && right == '}')
			return true;
		return false;
	}

	public String convert(String s, int nRows) {
		if (s == null || s.length() == 0 || nRows <= 0)
			return "";
		if (nRows == 1) {
			return s;
		}
		String subStr[] = new String[nRows];
		int period = 2 * nRows - 2;
		for (int i = 0; i < s.length(); i++) {
			if (i % period < nRows) {
				if (subStr[i % period] == null)
					subStr[i % period] = String.valueOf(s.charAt(i));
				else
					subStr[i % period] = subStr[i % period].concat(String.valueOf(s.charAt(i)));
			} else {
				subStr[period - i % period] = subStr[period - i % period].concat(String.valueOf(s.charAt(i)));
			}
		}
		String result = subStr[0];
		for (int i = 1; i < nRows; i++) {
			if (subStr[i] == null)
				break;
			result = result.concat(subStr[i]);
		}
		return result;
	}

	public int atoi(String str) {
        if(str == null || str.length() == 0)
            return 0;
        str = str.trim().toLowerCase();
        int result = 0;
        int flag = 1;
        int start = 0;
        if (str.charAt(0) == '-') {
            flag = -1;
            start = 1;
        }
        if (str.charAt(0) == '+') {
            start = 1;
        }
        for(int i = start; i < str.length(); i++) {
             if (str.charAt(i) >= '0' && str.charAt(i) <= '9') {
                 if (result > (Integer.MAX_VALUE - str.charAt(i) + '0' - 1) / 10) {
                	 System.out.println(str.charAt(i));
                	 System.out.println( Integer.MAX_VALUE - str.charAt(i) + '0' - 1);
                	 System.out.println(result);
                     return Integer.MAX_VALUE;
                 } else if (result < (Integer.MIN_VALUE + str.charAt(i) -'0' + 1) / 10) {
                	 System.out.println(Integer.MIN_VALUE);
                	 System.out.println(str.charAt(i));
                	 System.out.println( Integer.MIN_VALUE + str.charAt(i) - '0' + 1);
                	 System.out.println(result);
                     return Integer.MIN_VALUE;
                 } else {
                     if (flag == 1)
                        result = result * 10 + str.charAt(i) - '0';
                     else
                        result = result * 10 - str.charAt(i) + '0';
                 }
                 
             } else {
                 break;
             }
        }
        return result;
    }
	
	public String intToRoman(int num) {
        StringBuilder result = new StringBuilder("");
        int [] value = {1,5,10,50,100,500,1000};
        char [] symbol = {'I','V','X','L','C','D','M'};
        int consult;
        int i = value.length - 1;
        while (num > 0) {
            consult = num / value[i];
            num = num % value[i];
            switch (consult) {
                case 0:
                    break;
                case 1:
                    result.append(symbol[i]);
                    break;
                case 2:
                    result.append(symbol[i]).append(symbol[i]);
                    break;
                case 3:
                    result.append(symbol[i]).append(symbol[i]).append(symbol[i]);
                    break;
                case 4:
                    result.append(symbol[i]).append(symbol[i+1]);
            }
            i--;
        }
        return result.toString();
    }
	
	 public String addBinary(String a, String b) {
	        if (a == null || a.length() == 0) {
	            return b;
	        }
	        if (b == null || b.length() == 0) {
	            return a;
	        }
	        StringBuilder result = new StringBuilder();
	        Stack<Integer> s = new Stack<Integer>();
	        int i = a.length() - 1;
	        int j = b.length() - 1;
	        int carry = 0;
	        int value, digit;
	        while (i >= 0 && j >= 0) {
	            value = carry + a.charAt(i) - '0' + b.charAt(j) - '0';
	            digit = value % 2;
	            carry = value / 2;
	            s.push(digit);
	            i--;
	            j--;
	        }
	        while (i >= 0) {
	            value = carry + a.charAt(i) - '0';
	            digit = value % 2;
	            carry = value / 2;
	            s.push(digit);
	            i--;
	        }
	        while (j >= 0) {
	            value = carry + b.charAt(j) - '0';
	            digit = value % 2;
	            carry = value / 2;
	            s.push(digit);
	            j--;
	        }
	        if (carry > 0) {
	            s.push(carry);
	        }
	        System.out.println(s.empty());
	        while (!s.empty()) {
	            result.append(s.pop());
	        }
	        return result.toString();
	    }
	 
	 public void merge(int A[], int m, int B[], int n) {
	        int i = 0, j = 0 , k = 0;
	        int [] temp = new int[m + n];
	        while (i < A.length && j < B.length) {
	            if (A[i] <= B[j]) {
	                temp[k++] = A[i++];
	            } else {
	                temp[k++] = B[j++];
	            }
	        }
	        while (i < A.length) {
	            temp[k++] = A[i++];
	        }
	        while (j < B.length) {
	            temp[k++] = B[j++];
	        }
	        System.arraycopy(temp, 0, A, 0, m + n);
	    }
	
	 public List<Integer> get() {
		 ArrayList <Integer> list = new ArrayList<Integer>();
		 return list;
	 }
	 
	 public List<ArrayList<Integer>> get2() {
		 ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
		 return result;
 	 }
	 
	 public List<List<Integer>> get3() {
		 List<List<Integer>> result = new ArrayList<List<Integer>>();
		 return result;
 	 }
	 
	 public List<Integer> getRow(int rowIndex) {
	        List <Integer> preRow = new ArrayList <Integer>();
	        Integer [] result = new Integer[rowIndex + 1];
	        if (rowIndex < 0) return Arrays.asList(result);
	        preRow.add(1);
	        int colNum = 1;
	        while (colNum <= rowIndex + 1) {
	            for (int i = 0; i < colNum; i++) {
	                if (i == 0 || i == colNum - 1) {
	                    result[i] = 1;
	                } else {
	                	System.out.println(preRow.get(i-1));
	                	System.out.println(preRow.get(i));
	                    result[i] = preRow.get(i-1) + preRow.get(i);
	                    System.out.println(preRow.get(i-1));
	                	System.out.println(preRow.get(i));
	                }
	            }
	            preRow = Arrays.asList(result);
	            colNum++;
	        }
	        return Arrays.asList(result);
	    }
	 
	 public int lengthOfLongestSubstring(String s) {
	        if (s == null || s.length() == 0) {
	            return 0;
	        }
	        int [] pos = new int[256];
	        Arrays.fill(pos, -1);
	        int start = 0, i = 0, result = 0;
	        int c;
	        while (i < s.length()) {
	            c = s.charAt(i);
	            if (pos[c] >= start) {
	                start = pos[c] + 1;
	            }
	            pos[c] = i;
	            result = Math.max(result, i - start + 1);
	            i++;
	        }
	        return result;
	    }
	 
	 public static void main(String[] args) {
		Solution solu = new Solution();
		String s = new String();
		StringBuilder strBlder = new StringBuilder();
		
		Stack <Integer> stack = new Stack <Integer>();
		Queue <Integer> queue = new LinkedList<Integer>();
		List <Integer> list = new ArrayList<Integer>();
		Integer [] array = new Integer[10];
		Arrays.sort(array);
		list =  Arrays.asList(array);
		HashMap <Integer, Integer> map = new HashMap<Integer, Integer>();
		// solu.isValid(s);
		// Stack <Integer> stack = new Stack<Integer>();
		// stack.peek();
		// ArrayList <Integer> alist = new ArrayList<Integer>();
//		System.out.println(solu.convert("ABC", 2));
//		System.out.println(solu.atoi("2147483646"));
//		System.out.println(solu.intToRoman(1));
//		System.out.println(solu.addBinary("0", "0"));
//		int [] A = {};
//		int [] B = {1};
//		solu.merge(A, 0, B, 1);
//		solu.getRow(3);
		solu.lengthOfLongestSubstring("aa");
		
	}
} 