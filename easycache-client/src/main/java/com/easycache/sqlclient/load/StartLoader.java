package com.easycache.sqlclient.load;

import java.text.DecimalFormat;

public class StartLoader {

	static {
		Loader loader = new Loader();
		long start = System.currentTimeMillis();
		loader.loadData();
		long end = System.currentTimeMillis();
		DecimalFormat fnum = new DecimalFormat("##0.00");
		String time = fnum.format((float) (end - start) / 1000);
		System.out.println("load data time: " + time + " s");
	}
	
	public static void main(String[] args) {
		System.out.println("....");
	}
}
