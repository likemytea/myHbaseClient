/**
 * 
 */
package com.chenxing.myHbaseClient.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.chenxing.myHbaseClient.dao.Test1Dao;

/**
 * @author liuxing
 */
@RestController
@RequestMapping(value = "/test", method = RequestMethod.GET)
public class TestController {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	@Autowired
	Test1Dao dao;

	@RequestMapping(value = "/add", method = RequestMethod.GET)
	public String sayHi(@RequestParam String str, @RequestParam String tablename) {
		log.info(str + tablename);
		long start = System.currentTimeMillis();
		String res = null;
		try {
			dao.addRecordByBufferMutator(str, tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long end = System.currentTimeMillis();
		log.info("消耗时长 " + (start - end) + "毫秒");

		return res;

	}
}
