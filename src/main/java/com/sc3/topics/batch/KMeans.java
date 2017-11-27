package com.sc3.topics.batch;

import java.io.IOException;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sc3.topics.util.Props;
import com.sc3.topics.util.TDF;
import com.sc3.topics.util.Util;

/**
 * @author stephen.carton@gmail.com
 * @date Nov 18, 2017
 *
 */
public class KMeans {
	static {
		System.setProperty("log.file", "KMeans");
	}
	final static Logger logger = LoggerFactory.getLogger(KMeans.class);
	
	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		logger.info("Starting KMeans Clustering");
	
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
		
		Dataset<Row> rescaledData = idfModel
				.transform(featurizedData)
				.cache();
		org.apache.spark.ml.clustering.KMeans kmeans = new org.apache.spark.ml.clustering.KMeans().setK(2).setSeed(1L);
		KMeansModel model = kmeans.fit(rescaledData);
		double WSSSE = model.computeCost(rescaledData);
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center: centers) {
		  System.out.println(center);
		}		ss.stop();
		stopWatch.stop();
		logger.info("End of KMeans. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of KMeans. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
