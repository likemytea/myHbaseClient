package com.chenxing.myHbaseClient.dao;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
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
	 * 查询demo
	 * 
	 * @param tableName表名
	 * @param  String[3] 按照列过滤 第一个为columnfamily 第二个为列 column  第三个为列值 columnvalue
	 * @param startrow
	 * @param endrow
	 * @param Map<Integer, String> 行键过滤器
	 */
	public String select0(String tableName, String[] carr, String startrow, String endrow,
			Map<Integer, String> filterMap) throws IOException {
		StringBuffer sb = new StringBuffer();
		Connection connection = template.getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName), null);
		Scan scan = new Scan();
		scan = this.editFilters(scan, startrow, endrow, filterMap, carr);
		ResultScanner rScan = table.getScanner(scan);
		log.info("输出结果如下：");
		Result result = null;
		while ((result = rScan.next()) != null) {
			byte[] rowkey = result.getRow();
			log.info(new String(rowkey));
			sb.append(new String(rowkey));
			if (result.getValue("f_goods".getBytes(), "goodsName".getBytes()) != null) {
				log.info(new String(result.getValue("f_goods".getBytes(), "goodsName".getBytes())));
				sb.append(new String(result.getValue("f_goods".getBytes(), "goodsName".getBytes())));
			}
		}
		log.info("输出完毕。");
		rScan.close();
		return sb.toString();
	}

	private Scan editFilters(Scan scan, String startrow, String endrow, Map<Integer, String> filterMap, String[] carr) {
		// 分页过滤器TODO;新发现的HBASE自带分页，待研究。
		// Filter filter = new PageFilter(10);
		FilterList filterlist = new FilterList();
		// scan.setCacheBlocks(true);
		// scan.setCaching(5000);
		// scan.setBatch(3);

		if (startrow != null) {
			log.info("rokey range start set：" + startrow);
			scan.withStartRow(Bytes.toBytes(startrow), true);
		}
		if (endrow != null) {
			log.info("rokey range end set：" + endrow);
			scan.withStopRow(Bytes.toBytes(endrow), true);
		}
		if (filterMap != null && filterMap.size() > 0) {
			filterlist.addFilter(rowFilter(filterMap));
		}

		// 追加搜索条件(使用某column值过滤)
		if (carr != null) {
			log.info("SingleColumnValueFilter::::" + carr[0] + carr[1] + carr[2]);
			filterlist.addFilter(new SingleColumnValueFilter(Bytes.toBytes(carr[0]), Bytes.toBytes(carr[1]),
					CompareOperator.EQUAL, Bytes.toBytes(carr[2])));
		}
		// 只会取得每条数据的第一个kv，可以用于count，计算总数
		//filterlist.addFilter(new FirstKeyOnlyFilter());
		if (filterlist.size() > 0) {
			scan.setFilter(filterlist);
		}
		return scan;
	}

	private Filter rowFilter(Map<Integer, String> filterMap) {
		Filter filter = null;
		for (Integer key : filterMap.keySet()) {
			log.info("::行键过滤器值:" + filterMap.get(key));
			/** RowFilter 行健过滤器 start */
			// 提取rowkey以01结尾数据
			// Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new
			// RegexStringComparator(".*01$"));
			// 提取rowkey以包含201407的数据
			// Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new
			// SubstringComparator("201407"));
			/** RowFilter 行健过滤器 end */
			if (key > 0) {
				filter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(filterMap.get(key)));
			} else {
				filter = new RowFilter(CompareOperator.NOT_EQUAL, new SubstringComparator(filterMap.get(key)));
			}
		}
		return filter;
	}

	/**
	 * 添加一行数据 keep long time connect
	 * 
	 * @throws IOException
	 * 
	 */
	public void insert0(String str, String tableName, String rowkey, String cf, String col) throws IOException {
		// Table table = null;
		BufferedMutator bufferMutator = null;
		Connection connection = template.getConnection();
		try {
			// table = connection.getTable(TableName.valueOf(tableName), null);
			bufferMutator = connection.getBufferedMutator(TableName.valueOf(tableName));
			String key = null;
			if (rowkey != null) {
				key = rowkey;
			} else {
				key = PrimarykeyGenerated.generateId(false);
			}
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(str));
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
