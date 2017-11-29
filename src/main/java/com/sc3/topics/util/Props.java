package com.sc3.topics.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>Properties for a given run. Loaded either from a system property (-Dproperties=xxx) either form HDFS or the file system</p>
 * @author Steve Carton (stephen.carton@gmail.com) 
 * Jul 10, 2017
 *
 */
public class Props extends Properties {
	private static final long serialVersionUID = -250872066640539731L;
	final static Logger logger = LoggerFactory.getLogger(Props.class);

	/**
	 * Constructor tries to load properties from a location specified 
	 * in either an environment variable or a system property called "properties".
	 * The file indicated can be either an HDFS resources (hdfs://...), 
	 * a file system file (with a full path) or on the classpath. 
	 * 
	 * If no resource is specified, some individual properties can still be access as set via system properties:
	 * -Dzk.hosts=.....
	 * 
	 * @throws IOException if the resource can't be found anywhere.
	 */
	public Props() throws IOException { 
		String ppath = System.getProperty("properties");
		if (ppath==null || ppath.length()==0)
			ppath = System.getenv("properties");
		if (ppath!=null && ppath.length()>0) {
			InputStream in;
			if (ppath.startsWith("hdfs")) { // HDFS resource
				Path pp = new Path(ppath);
				FileSystem fs = FileSystem.get(new Configuration());
				if (fs.exists(pp) && fs.isFile(pp)) {
					logger.debug("HDFS Props file: {}",ppath);
					in = fs.open(pp);
				}
				else throw new IOException("HDFS File '"+ppath+"' doesn't exists or is not a file.");
			} else {
				File pf = new File(ppath);
				if (pf.exists() && pf.isFile()) { // File system resource
					in = new FileInputStream(ppath);
				}
				else if (Props.class.getClassLoader().getResource(ppath)!=null) { // classpath resource
					in = Props.class.getClassLoader().getResourceAsStream(ppath);
				}
				else throw new IOException("File '"+ppath+"' doesn't exists or is not a file or a classpath resource.");
			}
			load(in);
			in.close();
		}
	}
	/**
	 * <p>Returns the String value of a named property or the default provided.</p>
	 * @param n
	 * @return
	 */
	public String get(String n) {
		if (getProperty(n)!=null)
			return getProperty(n);
		if (System.getProperty(n)!=null)
			return (String)System.getProperty(n);
		return null;
	}
	public String get(String n, String d) {
		if (getProperty(n)!=null)
			return getProperty(n);
		if (System.getProperty(n)!=null)
			return (String)System.getProperty(n);
		return d;
	}
	public boolean has(String n) {
		return getProperty(n)!=null || System.getProperty(n)!=null;
	}
	public Boolean getBoolean(String n, Boolean d) {
		if (getProperty(n)!=null)
			return Boolean.valueOf(getProperty(n));
		if (System.getProperty(n)!=null)
			return Boolean.valueOf(System.getProperty(n));
		return d;
	}
	public Integer getInt(String n, Integer d) {
		if (getProperty(n)!=null && getProperty(n).length()>0)
			return Integer.parseInt(getProperty(n));
		if (System.getProperty(n)!=null && System.getProperty(n).length()>0)
			return Integer.parseInt(System.getProperty(n));
		return d;
	}
	public Long getLong(String n, Long d) {
		if (getProperty(n)!=null)
			return Long.parseLong(getProperty(n));
		if (System.getProperty(n)!=null)
			return Long.parseLong(System.getProperty(n));
		return d;
	}
	public Double getDouble(String n, Double d) {
		if (getProperty(n)!=null)
			return Double.parseDouble(getProperty(n));
		if (System.getProperty(n)!=null)
			return Double.parseDouble(System.getProperty(n));
		return d;
	}
	public long cacheSize() {
		return getLong("cache.size",10000L);
	}

	/**
	 * <p>Only for use in non-spark environments - fetches the zookeeper URLs from the properties.</p>
	 * @return
	 */
	public String zkHosts() {
		String p = get("zk.hosts", null);
		if (p==null)
			p = get("spark.deploy.zookeeper.url", null);
		if (p!=null)
			return p;
		throw new RuntimeException("No zk.hosts property found.");
	}
	/**
	 * <p>Returns an alternate set of the zookeeper URLs for a lexicon from the properties. Defaults to zk.hosts</p>
	 * @return
	 */
	public String lexZkHosts() {
		String p = get("zk.hosts.lex", null);
		return p!=null?p:zkHosts();
	}
	/**
	 * <p>For use in non-spark environments - fetches the zookeeper URLs from 
	 * the Spark Config or properties if that can't be found.</p>
	 * @return
	 */
	public String zkHosts(JavaSparkContext jsc) {
		String z = jsc.getConf().get("spark.deploy.zookeeper.url",null);
		if (z==null) 
			return zkHosts();
		else 
			return z;
	}
	public String collection() {
		String p = get("solr.collection", null);
		if (p!=null)
			return p;
		throw new RuntimeException("No solr.collection property found.");
	}
	public double scoreThreshold(double d) {
		return getDouble("score.threshold", d);
	}
	public Integer nearDupesMax() {
		return getInt("near.dupe.max", 100);
	}
	public boolean erase() {
		return getBoolean("solr.erase", false);
	}
	public String spark() {
		return get("spark", "");
	}
	public String classifierField() {
		String p = get("classifier.field", null);
		if (p!=null)
			return p+"_tag";
		throw new RuntimeException("No classifier.field property found.");
	}
	public String classifierScoreField() {
		String p = get("classifier.field", null);
		if (p!=null)
			return p+"_score";
		throw new RuntimeException("No classifier.field property found.");
	}
	public Integer blocksize() {
		return getInt("solr.blocksize", 1000);
	}
	public Integer limit() {
		return getInt("limit", Integer.MAX_VALUE-1);
	}
	public Integer start() {
		return getInt("start", 0);
	}
	public String logPath() {
		return get("log.path", null);
	}
	public String sourcePath() {
		String p = get("source.path", null);
		if (p!=null)
			return p+(p.endsWith(File.separator)?"":File.separator);
		throw new RuntimeException("No source.path property specified.");
	}
	public String modelPath() {
		String p = get("model.path", null);
		if (p!=null)
			return p;
		else
			throw new RuntimeException("Missing model.path property");
	}
	public String modelFile() {
		String p = get("model.file", null);
		if (p!=null)
			return p;
		else
			throw new RuntimeException("Missing model.file property");
	}
	public Level logLevel() {
		String p = get("log.level", "info");
		Level l;
		switch (p.toLowerCase()) {
			case "debug":
				l = Level.DEBUG;
				break;
			case "trace":
				l = Level.TRACE;
				break;
			case "info":
				l = Level.INFO;
				break;
			case "warn":
				l = Level.WARN;
				break;
			case "error":
				l = Level.ERROR;
				break;
			default:
				l = Level.INFO;
				break;
		}
		
		return l;
	}
	public String vectorFile() {
		String p = get("vector.file", null);
		if (p!=null)
			return p;
		else
			throw new RuntimeException("Missing vector.file property");
	}
	public String cachePath() {
		String p = get("cache.path", null);
		if (p!=null)
			return p;
		else
			throw new RuntimeException("Missing cache.path property");
	}
	public String lexicon() {
		String p = get("lexicon", "corpus");
		if (p!=null)
			return p;
		else 
			throw new RuntimeException("Missing lexicon property");
	}
	public Integer threads() {
		return getInt("threads", 1);
	}
	public String executorMemory() {
		return get("spark.executor.memory", "20g");
	}
	public String driverMemory() {
		return get("spark.driver.memory", "20g");
	}
	public double[] splits() {
		String[] tks = get("splits", ".5,.5").split(",");
		double[] s= new double[2];
		s[0] = Double.parseDouble(tks[0]);
		s[1] = Double.parseDouble(tks[1]);
		return s;
	}
	public String dupeField() {
		String p = get("dupe.field", null);
		if (p!=null)
			return p;
		throw new RuntimeException("No dupe.field property found.");
	}
	public int features() {
		return getInt("features", 10000);
	}
	public int k() {
		return getInt("k", 2);
	}
	public int iterations() {
		return getInt("iterations", 100);
	}
	public int ngramWidth() {
		return getInt("ngram.width", 3);
	}
}
