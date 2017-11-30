package com.sc3.topics.batch;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
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

import scala.collection.mutable.WrappedArray;

/**
 * Sample LDA clustering, B version. Uses countVectorizer to create TF vectors
 *  Arguments specified as properties: -Dthreads=2
 * -DLog.Path=/home/steve/logs -Dsource.path=/home/steve/data/enron-clean -Dk=2
 * -Diterations=100
 * 
 * @author stephen.carton@gmail.com
 * @date Nov 18, 2017
 *
 */
public class LDANgramBRun {
	static {
		System.setProperty("log.file", "LDANgramB");
	}

	final static Logger logger = LoggerFactory.getLogger(LDANgramBRun.class);

	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		logger.info("Starting LDA NGram B Clustering");

		SparkSession ss = Util.createSparkSession(props, "LDANgramB");
		Dataset<Row> ngramDS = TDF.loadDataSetAsNgrams(props, ss);
		
		CountVectorizerModel cvModel = new CountVectorizer()
				.setInputCol("tokens")
				.setOutputCol("tf-vec")
//				.setVocabSize(3)
				.setMinDF(2)
				.fit(ngramDS);
		Dataset<Row> tfDS = cvModel.transform(ngramDS);

		IDF idf = new IDF()
				.setInputCol("tf-vec")
				.setOutputCol("tfidf-vec");
		IDFModel idfModel = idf.fit(tfDS);
		Dataset<Row> tfidfDS = idfModel.transform(tfDS);

		LDA lda = new LDA()
				.setFeaturesCol("tfidf-vec")
				.setK(props.k())
				.setMaxIter(props.iterations());
		LDAModel model = lda.fit(tfidfDS);
		
		String[] vocab = cvModel.vocabulary();
//		logger.debug("{}",String.join(" ", vocab));
		Dataset<Row> topicIndices = model.describeTopics();
		topicIndices.printSchema();
		topicIndices.createOrReplaceTempView("topicIndices");
		Dataset<Row> topicI = ss.sql("SELECT termIndices FROM topicIndices");
//		topicI.printSchema();
		topicI.foreach(ta -> logit(ta,vocab));
		ss.stop();
		stopWatch.stop();
		logger.info("End of LDANgramB. Elapsed time: " + DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of LDANgramB. Elapsed time: " + DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}

	private static void logit(Row ta, String[] vocab) {
		List<Integer> raw = ta.getList(0);
		logger.info("{}",raw);
		for (int i=0; i<raw.size(); i++) { 
			logger.info("{}: {}/{}",i, raw.get(i),vocab[raw.get(i)]);
		}
	}
}
