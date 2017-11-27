package com.sc3.topics.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Misc static Utilities.
 * 
 * @author Steve Carton (stephen.carton@gmail.com) Jul 14, 2017
 *
 */
public final class Util {
	final static Logger logger = LoggerFactory.getLogger(Util.class);

	private Util() {}

	/**
	 * makes a String out of a JAVA stack trace
	 * @param e
	 *            Exception to trace
	 * @return resulting trace as a String
	 */
	public static String stackTrace(Exception e) {
		if (e == null)
			return ("No StackTrace available on NULL exception.");
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
	public static void setLogging(Level l) {
		org.apache.log4j.Logger.getLogger("com").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("edu").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR);	
		org.apache.log4j.Logger.getLogger("cobra").setLevel(l);
	}
	public static JavaSparkContext createSparkContext(Props props, String appName) throws IOException {
		SparkConf sparkConf;
		if (props.spark().startsWith("local")) {
			String c = props.spark().equals("local")?"local["+props.threads()+"]":props.spark();
			sparkConf = new SparkConf()
				.setAppName(appName)
				.setMaster(c);
		} else {
			sparkConf = new SparkConf()
				.setAppName(appName);
		}
		return new JavaSparkContext(sparkConf);
	}
	public static SparkSession createSparkSession(Props props, String appName) throws IOException {
		return SparkSession.builder()
	     .master("local")
	     .appName(appName)
	     .config("spark.executor.cores", props.threads())
	     .getOrCreate();
	}
}
