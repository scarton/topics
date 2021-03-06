package com.sc3.topics.batch;

import java.io.IOException;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.clustering.KMeans;
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
 * Sample KMeans clustering. 
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
public class KMeansRun {
	static {
		System.setProperty("log.file", "KMeans");
	}
	final static Logger logger = LoggerFactory.getLogger(KMeansRun.class);
	
	public static void main(String[] args) throws IOException {
		Props props = new Props();
		Util.setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		logger.info("Starting KMeans Clustering");
	
		SparkSession ss = Util.createSparkSession(props, "KMeans");
		Dataset<Row> textData = TDF.loadDataSet(props, ss);
		HashingTF hashingTF = new HashingTF()
				  .setInputCol("tokens")
				  .setOutputCol("rawFeatures")
				  .setNumFeatures(props.features());
				
		Dataset<Row> featurizedData = hashingTF.transform(textData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		
		Dataset<Row> rescaledData = idfModel
				.transform(featurizedData)
				.cache();
		KMeans kmeans = new KMeans()
				.setK(props.k())
				.setMaxIter(props.iterations())
				.setSeed(1L);
		KMeansModel model = kmeans.fit(rescaledData);
		double WSSSE = model.computeCost(rescaledData);
		logger.info("Within Set Sum of Squared Errors = " + WSSSE);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		logger.info("Cluster Centers: ");
		for (Vector center: centers) {
			logger.info("{}",center);
		}
		ss.stop();
		stopWatch.stop();
		logger.info("End of KMeans. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of KMeans. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
