package com.sc3.topics.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sc3.topics.TextStuff;

/**
 * @author stephen.carton@gmail.com
 * @date Nov 18, 2017
 *
 */
public final class TDF {
	final static Logger logger = LoggerFactory.getLogger(TDF.class);
	final static Pattern sentenceRX = Pattern.compile("\\r?\\n");

	public static TextStuff tokenize(String sp, String id) {
		String text = "";
		try {
			text = FileUtils.readFileToString(new File(sp+id));
		} catch (IOException e) {
			logger.error(Util.stackTrace(e));
		}
		return TextStuff.apply(id, text.split("\\s"));
	}
	public static TextStuff ngramize(int ngw, String sp, String id) {
		String text = "";
		try {
			text = FileUtils.readFileToString(new File(sp+id));
		} catch (IOException e) {
			logger.error(Util.stackTrace(e));
		}
		ArrayList<String> ngTokens=new ArrayList<>();
		if (text!=null && text.length()>0) {
			for (String line : sentenceRX.split(text)) {
				if (line.length()>0) {
					logger.trace("Sentence: {}",line);
					line = line.trim().replaceAll("\\s+", " ");
					if (line!=null && line.length()>0) {
						String[] tks = line.split(" ");
						if (tks.length>ngw) {
							for (int i=0; i<tks.length-ngw; i++) {
								StringBuilder sb = new StringBuilder();
								for (int j=0; j<ngw; j++) {
									if (sb.length()>0)
										sb.append('_');
									sb.append(tks[i+j]);
								}
								ngTokens.add(sb.toString());
							}
						} else {
							ngTokens.add(String.join("_", tks));
						}
					}
				}
			}
		}
		return TextStuff.apply(id, ngTokens.toArray(new String[0]));
	}
	@SuppressWarnings("resource")
	public static Dataset<Row> loadDataSet(Props props, SparkSession ss) throws IOException {
		String sp = props.sourcePath();
		logger.debug("Sources: {}",sp);
		List<String> ids = Files.walk(Paths.get(sp))
	    	.filter(p -> p.toString().endsWith(".txt"))
	    	.map(p -> p.toString().substring(sp.length()))
	    	.collect(Collectors.toList());
		JavaRDD<TextStuff> tokensRDD = new JavaSparkContext(ss.sparkContext())
				.parallelize(ids, props.threads())
				.map(id -> TDF.tokenize(sp, id));
		Dataset<Row> textData = ss.createDataFrame(tokensRDD, TextStuff.class);
		return textData;
	}
	@SuppressWarnings("resource")
	public static Dataset<Row> loadDataSetAsNgrams(Props props, SparkSession ss) throws IOException {
		String sp = props.sourcePath();
		logger.debug("Sources: {}",sp);
		List<String> ids = Files.walk(Paths.get(sp))
	    	.filter(p -> p.toString().endsWith(".txt"))
	    	.map(p -> p.toString().substring(sp.length()))
	    	.collect(Collectors.toList());
		JavaRDD<TextStuff> tokensRDD = new JavaSparkContext(ss.sparkContext())
				.parallelize(ids, props.threads())
				.map(id -> TDF.ngramize(props.ngramWidth(),sp, id));
		Dataset<Row> textData = ss.createDataFrame(tokensRDD, TextStuff.class);
		return textData;
	}
}
