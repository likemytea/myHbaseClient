package com.chenxing.myHbaseClient.designPattern;

/**
 * 单例模式<br>
 * 1.单例类只有一个实例<br>
 * 2.自己创建自己<br>
 * @author liuxing
 */
public class Singleton {
	private static Singleton singleton;

	/**
	 * 构造方法私有化
	 */
	private Singleton() {

	}

	public static Singleton getInstance() {
		if (singleton == null) {
			singleton = new Singleton();
		}
		return singleton;
	}
}
