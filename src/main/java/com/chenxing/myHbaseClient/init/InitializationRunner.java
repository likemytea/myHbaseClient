package com.chenxing.myHbaseClient.init;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.chenxing.myHbaseClient.dao.HbaseClientTemplate;

/**
 * Description: 系统初始化
 * 
 * @author liuxing
 * @date 2018年9月26日
 * @version 1.0
 */
@Component
public class InitializationRunner implements ApplicationRunner {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	@Value("${hadoop.cfg.dir}")
	String hdpdir;
	@Value("${hadoop.hbase.cfg.dir}")
	String hbasedir;
	@Value("${hadoop.hbase.connect.open}")
	String hbaseOpen;
	@Value("${hadoop.hbase.table.testTable0}")
	String table0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.boot.ApplicationRunner#run(org.springframework.boot.
	 * ApplicationArguments)
	 */
	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("系统初始化 start ...");
		initializeHbase();
		log.info("系统初始化 end!");
	}

	/**
	 * hbase连接初始化
	 */
	private void initializeHbase() {
		Configuration config = HBaseConfiguration.create();
		config.addResource(new Path(hbasedir, "hbase-site.xml"));
		config.addResource(new Path(hdpdir, "core-site.xml"));
		HbaseClientTemplate.conf = config;
		// new HbaseDataSource(HbaseDataSource.conf, table0);
	}
}
