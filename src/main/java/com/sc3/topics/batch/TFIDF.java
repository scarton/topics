package com.sc3.topics.batch;
import java.io.IOException;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sc3.topics.util.Props;
import com.sc3.topics.util.TDF;
import com.sc3.topics.util.Util;

public class TFIDF {
	static {
		System.setProperty("log.file", "TFIDF");
	}
	final static Logger logger = LoggerFactory.getLogger(TFIDF.class);
	

	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		SparkSession ss = Util.createSparkSession(props, "TFIDF");
		Dataset<Row> textData = TDF.loadDataSet(props, ss);
		
		
		HashingTF hashingTF = new HashingTF()
		  .setInputCol("tokens")
		  .setOutputCol("rawFeatures")
		  .setNumFeatures(props.features());
		
		Dataset<Row> featurizedData = hashingTF.transform(textData);
		// alternatively, CountVectorizer can also be used to get term frequency vectors
		
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		rescaledData.select("id", "features").show(10,false);
		ss.stop();
		logger.info("End of TFIDF. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of TFIDF. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
