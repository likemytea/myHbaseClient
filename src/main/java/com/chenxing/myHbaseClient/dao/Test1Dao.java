package com.chenxing.myHbaseClient.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.chenxing.common.distributedKey.PrimarykeyGenerated;
import com.chenxing.myHbaseClient.util.ThreadPool;

public class Test1Dao {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	public static Configuration conf = null;
	/**
	 * 添加一行数据 keep long time connect
	 * 
	 * @throws IOException
	 * 
	 */
	public void addRecordByBufferMutator(String mqStr, String tableName) throws IOException {
		Connection connection = null;
		// Table table = null;
		BufferedMutator bufferMutator = null;
		JSONArray jr = null;

		try {
			connection = ConnectionFactory.createConnection(conf, ThreadPool.geteService());
			log.info("创建hbase connection 成功");
			// table = connection.getTable(TableName.valueOf(tableName), null);
			log.info("获取 hbase table 成功");
			bufferMutator = connection.getBufferedMutator(TableName.valueOf(tableName));
			String key = PrimarykeyGenerated.generateId(false);
			Put put = new Put(Bytes.toBytes(key));
			jr = JSON.parseArray(mqStr);
			put.addColumn(Bytes.toBytes("familly"), Bytes.toBytes("columns"), Bytes.toBytes(jr.getString(4)));
			log.info("实例化 bufferMutator 成功");
			log.info("hbase 初始化 全部完成！");
			bufferMutator.mutate(put);
			// 调用flush()方法会把put中的数据立刻刷到hbase的memstore里边，不调用的话，会保存到程序服务的缓存中
			bufferMutator.flush();
			log.info("Insert-key: " + key + " to table succ.");
		} catch (IOException e) {
			log.error("hbase初始化失败，失败原因：" + e.getMessage());
			e.printStackTrace();
		} finally {
			bufferMutator.close();
			connection.close();
		}
	}

}
