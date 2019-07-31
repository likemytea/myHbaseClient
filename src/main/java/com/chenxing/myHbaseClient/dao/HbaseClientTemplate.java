package com.chenxing.myHbaseClient.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.chenxing.myHbaseClient.util.ThreadPool;
/**
 * 创建连接是一个重量级操作。连接实现是线程安全的，因此客户端可以创建一个连接，并与不同的线程共享它。
 * Table和Admin实例是轻量级的，并且不是线程安全的。通常，每个客户端应用程序都实例化一个连接，
 * 每个线程都将获得自己的Table实例。不推荐使用Table和Admin的缓存或池。
 * 
 * */
@Component
public class HbaseClientTemplate {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	public static Configuration conf = null;
	private static Connection connection = null;

	public Connection getConnection() {
		try {
			if (connection != null && connection.isAborted()) {
				log.error("严重异常：hbase connection 状态  isAborted");
				if (!connection.isClosed()) {
					connection.close();
				}
				connection = ConnectionFactory.createConnection(conf, ThreadPool.geteService());
				return connection;
			} else if (connection == null || connection.isClosed()) {
				log.info("hbase client 创建到hbaseserver的连接...");
				connection = ConnectionFactory.createConnection(conf, ThreadPool.geteService());
				log.info("hbase client 创建到连接成功。");
				return connection;
			}
		} catch (IOException e) {
			log.error("hbase获取连接失败，原因：" + e.getMessage());
			e.printStackTrace();
			return null;
		}
		return connection;
	}
}
