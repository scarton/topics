package com.sc3.topics.batch;

import java.io.IOException;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sc3.topics.util.Props;
import com.sc3.topics.util.TDF;
import com.sc3.topics.util.Util;

import scala.collection.mutable.WrappedArray;

/**
 * Sample LDA clustering. Arguments specified as properties: -Dthreads=2
 * -DLog.Path=/home/steve/logs -Dsource.path=/home/steve/data/enron-clean -Dk=2
 * -Diterations=100
 * 
 * @author stephen.carton@gmail.com
 * @date Nov 18, 2017
 *
 */
public class LDANgramRun {
	static {
		System.setProperty("log.file", "LDANgram");
	}

	final static Logger logger = LoggerFactory.getLogger(LDANgramRun.class);

	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		logger.info("Starting LDA Clustering");

		SparkSession ss = Util.createSparkSession(props, "LDANgramRun");
		Dataset<Row> textData = TDF.loadDataSetAsNgrams(props, ss);
		HashingTF hashingTF = new HashingTF()
				.setInputCol("tokens")
				.setOutputCol("rawFeatures")
				.setNumFeatures(props.features());

		Dataset<Row> tfData = hashingTF.transform(textData);
		IDF idf = new IDF()
				.setInputCol("rawFeatures")
				.setOutputCol("features");
		IDFModel idfModel = idf.fit(tfData);

		Dataset<Row> tfidfData = idfModel.transform(tfData).cache();

		LDA lda = new LDA()
				.setFeaturesCol("features")
				.setK(props.k())
				.setMaxIter(props.iterations());
		LDAModel model = lda.fit(tfidfData);
		Matrix topicM = model.topicsMatrix();
		for (int topic = 0; topic < 3; topic++) {
			logger.info("Topic " + topic + ":");
			for (int word = 0; word < model.vocabSize(); word++) {
				logger.info(" " + topicM.apply(word, topic));
			}
		}

		// Describe topics.
		// Dataset<Row> topics = model.describeTopics(3);
		// topics.printSchema();
		// topics.map(f -> TDF.log(f));
		// logger.info("The topics described by their top-weighted terms:");
		// topics.takeAsList(10).forEach(f -> logger.info("{}",f));
		// topics.show(false);

		// Shows the result.
		logger.info("The Results:");
		Dataset<Row> transformed = model.transform(tfidfData);
		transformed.printSchema();
		transformed.takeAsList(10).forEach(f -> logger.info("{} - {}", f.get(0), ((WrappedArray<String>) f.get(1))));
		// transformed.show(false);
		ss.stop();
		stopWatch.stop();
		logger.info(
				"End of LDA. Elapsed time: " + DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println(
				"End of LDA. Elapsed time: " + DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
