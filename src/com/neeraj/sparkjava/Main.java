/**
 * Main
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Main Class. To run Spark Java Programming Examples.
 * 
 * @author neeraj
 *
 */
public class Main {
	/**
	 * Main Class implements the main method to test and run Spark Java
	 * Programming Examples. 
	 * - How to create SparkContext and SparkSession. 
	 * - Taking data from arrays and external file source. 
	 * - Spark Map Transformation. 
	 * - Spark Filter Transformation. 
	 * - Spark FlatMap Transformation. 
	 * - Compare Map and FlatMap. 
	 * - Set Operations. 
	 * - Spark Reduce Transformation. 
	 * - Spark Aggregate Transformation. 
	 * - Using Functions in Spark Transformation. 
	 * - Key Value RDD.
	 * - Using HDFS
	 */

	/**
	 * Runs the Spark Java Programming Examples.
	 * 
	 * @param args
	 *            Takes nothing.
	 */
	public static void main(String[] args) {

		/* Creating SparkContext. */
		CreateSpark spark = new CreateSpark();
		JavaSparkContext sparkContext = spark.context("Spark Programming", "local");

		/* Using Array Data. */
		ArrayData arrayData = new ArrayData(sparkContext);
		arrayData.callArrayData();

		/* Accessing external file source. */
		ExternalFileData exFileData = new ExternalFileData(sparkContext);
		exFileData.callFileData("/home/neeraj/cars.csv");

		/* Spark Map Transformation. */
		SparkMap sparkMap = new SparkMap(sparkContext);
		sparkMap.mapReplace("American", "AMERICAN");

		/* Spark Filter Transformation. */
		SparkFilter sparkFilter = new SparkFilter(sparkContext);
		sparkFilter.callFilter("audi");

		/* Spark FlatMap Transformation. */
		SparkFlatMap flatMap = new SparkFlatMap(sparkContext);
		flatMap.callFlatMap();

		/* Comparing Map and FlatMap. */
		CompareMapAndFlatMap compareMaps = new CompareMapAndFlatMap(sparkContext);
		compareMaps.compare();
		/* Set Operations. */
		SetOperations setOperations = new SetOperations(sparkContext);
		setOperations.callSetOp();

		/* Spark Reduce Transformation. */
		Reduce sparkReduce = new Reduce(sparkContext);
		sparkReduce.shortestLine();
		sparkReduce.sum();

		/* Spark Aggregate Transformation. */
		Aggregation aggregate = new Aggregation(sparkContext);
		aggregate.sum();
		aggregate.sumAndProduct();

		/* Using Functions in Spark Transformations. */
		Functions.example1(sparkContext);
		Functions.example2(sparkContext);

		/* Key Value RDD. */
		KeyValueRDD keyValueRdd = new KeyValueRDD(sparkContext);
		keyValueRdd.callKVRDD();

		/* Using HDFS. Run 'start-all.sh' from Terminal to start Hadoop. */
		UsingHDFS hdfsData = new UsingHDFS(sparkContext);
		JavaRDD<String> carData = hdfsData.readHDFS("hdfs://localhost:5432/CarDetails/part-00000")
				.filter(x -> x.contains("audi"));
		hdfsData.saveToHDFS(carData, "hdfs://localhost:5432/CarDetails/Audi");

		/* Closing SparkContext. */
		sparkContext.close();

	}

}
