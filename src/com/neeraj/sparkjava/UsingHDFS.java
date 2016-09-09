/**
 * UsingHDFS
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class UsingHDFS. Example on How to use HDFS Data.
 * 
 * @author neeraj
 *
 */
public class UsingHDFS {
	/**
	 * Class UsingHDFS implements two functions 'saveToHDFS()' and 'readHDFS()'
	 * to describe the usage of HDFS.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create JavaRDD from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	private int noOfPartitions = 1;

	/**
	 * Default Constructor. Creates 'SparkSession' if UsingHDFS object is created
	 * without 'SparkContext'.
	 */
	UsingHDFS() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("Spark HDFS Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	UsingHDFS(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 * @param noOfPartitions
	 *            takes the no. of partition.
	 */
	UsingHDFS(SparkSession sparkSession, int noOfPartitions) {
		this.sparkSession = sparkSession;
		this.noOfPartitions = noOfPartitions;
	}

	/**
	 * Save the 'JavaRDD' hdfsData to the specified 'HDFS' location.
	 * 
	 * @param hdfsData
	 *            takes a generic type JavaRDD.
	 * @param path
	 *            takes the HDFS location.
	 */
	public <T> void saveToHDFS(JavaRDD<T> hdfsData, String path) {

		/* Checking path contains HDFS location or not. */
		if (!path.contains("hdfs://")) {
			System.err.println("\nERROR: Invalid HDFS Path\n"); // ErrorMessage.
			java.lang.System.exit(0); 							// Exit.
		}

		/* Save to HDFS. */
		hdfsData.saveAsTextFile(path);

	}

	/**
	 * Read a 'HDFS' file to a 'JavaRDD'.
	 * 
	 * @param filePath
	 *            path to a HDFS file.
	 *            (eg:hdfs://localhost:5432/newOutput/part-00000).
	 * @return JavaRDD of String.
	 */
	public JavaRDD<String> readHDFS(String filePath) {

		/* Creating a JavaRDD of String. */
		JavaRDD<String> hdfsData;

		/* Checking path contains HDFS location or not. */
		if (!filePath.contains("hdfs://")) {
			System.err.println("\nERROR: Invalid HDFS Path"); // ErrorMessage.
			java.lang.System.exit(0); 						  // Exit.
		}

		if (sparkContext == null) { // Checking sparkContext is null.

			/* Reading HDFS file to JavaRDD. */
			hdfsData = sparkSession.sparkContext().textFile(filePath, noOfPartitions).toJavaRDD();

		} else {

			/* Reading HDFS file to JavaRDD. */
			hdfsData = sparkContext.textFile(filePath);

		}
		/* Returns the JavaRDD to calling method. */
		return hdfsData;
	}

}
