package com.chenxing.myHbaseClient.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.chenxing.common.distributedKey.PrimarykeyGenerated;

@Component
public class Test2Dao {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	@Autowired
	HbaseClientTemplate template;

	/**
	 * 添加一行数据 keep long time connect
	 * 
	 * @throws IOException
	 * 
	 */
	public void insert0(String str, String tableName) throws IOException {
		// Table table = null;
		BufferedMutator bufferMutator = null;
		Connection connection = template.getConnection();
		try {
			// table = connection.getTable(TableName.valueOf(tableName), null);
			bufferMutator = connection.getBufferedMutator(TableName.valueOf(tableName));
			String key = PrimarykeyGenerated.generateId(false);
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes("f_goods"), Bytes.toBytes("goodsName"), Bytes.toBytes(str));
			bufferMutator.mutate(put);
			// 调用flush()方法会把put中的数据立刻刷到hbase的memstore里边，不调用的话，会保存到程序服务的缓存中
			bufferMutator.flush();
			log.info("Insert-key: " + key + " to table succ.");
		} catch (IOException e) {
			log.error("hbase初始化失败，失败原因：" + e.getMessage());
			e.printStackTrace();
		} finally {
			bufferMutator.close();
			log.info("!!!关闭 bufferMutator success");
		}
	}
}
