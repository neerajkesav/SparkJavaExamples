/**
 * CreateSpark
 */
package com.neeraj.sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class CreateSpark. Creates 'JavaSparkContext' and 'SparkSession'.
 * 
 * @author neeraj
 *
 */
public class CreateSpark {
	/**
	 * Class CreateSpark implements two functions 'context()' and 'session()' to
	 * create 'JavaSparkContext and SparkSession'.
	 */

	/**
	 * Function 'context()' creates 'JavaSparkContext' using the specified 'Spark
	 * Configuration'.
	 * 
	 * @param appName
	 *            Name of the Spark Application
	 * @param master
	 *            Specifies where the Spark Application runs. Takes values
	 *            'local', local[*], 'master'.
	 */
	public JavaSparkContext context(String appName, String master) {

		/* Creating Spark Configuration */
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

		/* Creating Spark Context */
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		return sparkContext;

	}

	/**
	 * Function 'session()' creates 'SparkSession' using the specified Spark
	 * Configuration.
	 * 
	 * @param appName
	 *            Name of the Spark Application
	 * @param master
	 *            Specifies where the Spark Application runs. Takes values
	 *            'local', local[*], 'master'.
	 */
	public SparkSession session(String appName, String master) {

		/* Creating Spark Configuration */
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

		/* Creating Spark Session */
		SparkSession sparkSession = SparkSession.builder().appName(appName).config(conf).getOrCreate();

		return sparkSession;
	}

}
