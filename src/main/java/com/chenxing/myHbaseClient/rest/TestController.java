/**
 * 
 */
package com.chenxing.myHbaseClient.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.chenxing.myHbaseClient.dao.Test2Dao;

/**
 * @author liuxing
 */
@RestController
@RequestMapping(value = "/test", method = RequestMethod.GET)
public class TestController {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	@Autowired
	Test2Dao dao;

	// http://172.16.14.241:8080/test/select?tablename=t_order&carr=f_goods,goodsName,121212&startrow=18092712040300001&endrow=18092712071400004&rowfilter=1,180927
	@RequestMapping(value = "/select", method = RequestMethod.GET)
	public String select(@RequestParam String tablename, @RequestParam(required = false) String carr,
			@RequestParam(required = false) String startrow, @RequestParam(required = false) String endrow,
			@RequestParam(required = false) String rowfilter) {
		long start = System.currentTimeMillis();
		String[] a = null;
		if (carr != null) {
			a = carr.split(",");
		}
		Map<Integer, String> filterMap = null;
		if (rowfilter != null) {
			String[] filterArr = rowfilter.split("hhhh");
			filterMap = new HashMap<Integer, String>();
			for (int j = 0; j < filterArr.length; j++) {
				String[] x = filterArr[j].split(",");
				filterMap.put(Integer.parseInt(x[0]), x[1]);
			}
		}
		try {
			dao.select0(tablename, a, startrow, endrow, filterMap);
		} catch (IOException e) {
			log.error("严重异常：" + e.getMessage());
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		log.info("消耗时长 " + (start - end) + "毫秒");
		return null;
	}

	@RequestMapping(value = "/add", method = RequestMethod.GET)
	public String insert0(@RequestParam String str, @RequestParam String tablename) {
		log.info(str + tablename);
		long start = System.currentTimeMillis();
		String res = null;
		try {
			dao.insert0(str, tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long end = System.currentTimeMillis();
		log.info("消耗时长 " + (start - end) + "毫秒");

		return res;

	}

}
