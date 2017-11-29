package com.sc3.topics.batch;

import java.io.IOException;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
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

/**
 * Sample LDA clustering. 
 * Arguments specified as properties:
 * -Dthreads=2
 * -DLog.Path=/home/steve/logs
 * -Dsource.path=/home/steve/data/enron-clean
 * -Dk=2
 * -Diterations=100
 * @author stephen.carton@gmail.com
 * @date Nov 18, 2017
 *
 */
public class LDARun {
	static {
		System.setProperty("log.file", "LDA");
	}
	final static Logger logger = LoggerFactory.getLogger(LDARun.class);
	
	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		logger.info("Starting LDA Clustering");
	
		SparkSession ss = Util.createSparkSession(props, "LDA");
		Dataset<Row> textData = TDF.loadDataSet(props, ss);
		HashingTF hashingTF = new HashingTF()
				  .setInputCol("tokens")
				  .setOutputCol("rawFeatures")
				  .setNumFeatures(props.features());

		Dataset<Row> tfData = hashingTF.transform(textData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(tfData);
		
		Dataset<Row> tfidfData = idfModel
				.transform(tfData)
				.cache();

		LDA lda = new LDA()
				.setFeaturesCol("features")
				.setK(props.k())
				.setMaxIter(props.iterations());
		LDAModel model = lda.fit(tfidfData);

		double ll = model.logLikelihood(tfidfData);
		double lp = model.logPerplexity(tfidfData);
		logger.info("The lower bound on the log likelihood of the entire corpus: " + ll);
		logger.info("The upper bound on perplexity: " + lp);

		// Describe topics.
		Dataset<Row> topics = model.describeTopics(3);
		logger.info("The topics described by their top-weighted terms:");
		topics.takeAsList(10).forEach(f -> logger.info("{}",f));
//		topics.show(false);

		// Shows the result.
		Dataset<Row> transformed = model.transform(tfidfData);
		transformed.takeAsList(10).forEach(f -> logger.info("{}",f));
//		transformed.show(false);
		ss.stop();
		stopWatch.stop();
		logger.info("End of LDA. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of LDA. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
