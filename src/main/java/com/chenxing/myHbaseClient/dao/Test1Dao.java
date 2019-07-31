package com.chenxing.myHbaseClient.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.chenxing.common.distributedKey.PrimarykeyGenerated;
import com.chenxing.myHbaseClient.util.ThreadPool;

@Component
public class Test1Dao {
	private final Logger log = LoggerFactory.getLogger(this.getClass());

	/**
	 * 不推荐的用法
	 * @throws IOException
	 * 
	 */
	public void addRecordByBufferMutator(String str, String tableName) throws IOException {
		Connection connection = null;
		// Table table = null;
		BufferedMutator bufferMutator = null;

		try {
			connection = ConnectionFactory.createConnection(HbaseClientTemplate.conf, ThreadPool.geteService());
			log.info("创建hbase connection 成功");
			// table = connection.getTable(TableName.valueOf(tableName), null);
			bufferMutator = connection.getBufferedMutator(TableName.valueOf(tableName));
			log.info("获取 hbase connection bufferMutator 成功");
			String key = PrimarykeyGenerated.generateId(false);
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes("f_goods"), Bytes.toBytes("goodsName"), Bytes.toBytes(str));
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
			log.info("关闭hbase conn success");
		}
	}

}
