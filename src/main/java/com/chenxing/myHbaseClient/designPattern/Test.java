package com.chenxing.myHbaseClient.designPattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	public static void main(String[] args) {
		Test a = new Test();
		a.test();
	}

	public void test() {
		log.info("" + Singleton.getInstance());
	}
}
