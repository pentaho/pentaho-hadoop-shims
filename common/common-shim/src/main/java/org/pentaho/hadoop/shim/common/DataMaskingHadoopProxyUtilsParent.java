package org.pentaho.hadoop.shim.common;

import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;

/**
 * kerberos auth
 * 
 * @author yangchao
 *
 */
public class DataMaskingHadoopProxyUtilsParent {
	protected static ConcurrentHashMap<String, Subject> SUBJECT_MAP = new ConcurrentHashMap<>();

	/**
	 * 获取subject
	 * 
	 * @return
	 */
	public static ConcurrentHashMap<String, Subject> getSUBJECT_MAP() {
		return SUBJECT_MAP;
	}

	public static void put(String k, Subject v) {
		SUBJECT_MAP.put(k, v);
	}
}
