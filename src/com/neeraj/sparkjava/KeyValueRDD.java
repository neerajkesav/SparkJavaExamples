/**
 * KeyValueRDD
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Class KeyValueRDD. Example on how to use Key Value RDD.
 * 
 * @author neeraj
 *
 */
public class KeyValueRDD {

	/**
	 * Class KeyValueRDD implements function 'callKVRDD()' to describe the usage
	 * of Key Value RDD.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create 'JavaRDD' from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	/**
	 * Default Constructor. Creates 'SparkSession' if KeyValueRDD object is
	 * created without 'SparkContext'.
	 */
	KeyValueRDD() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("Key Value RDD Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	KeyValueRDD(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 */
	KeyValueRDD(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Calculating average weight of car models.
	 */
	public void callKVRDD() {
		
		/* Creating JavaRDD of String. */
		JavaRDD<String> carData;

		if (sparkContext == null) {// Checking sparkContext is null.

			/*
			 * Assign records from cars.csv to carData using sparkSession and
			 * caching to memory.
			 */
			carData = sparkSession.sparkContext().textFile("/home/neeraj/cars.csv", 1).toJavaRDD().cache();

		} else {

			/*
			 * Assign records from cars.csv to carData using sparkContext and
			 * caching to memory.
			 */
			carData = sparkContext.textFile("/home/neeraj/cars.csv").cache();

		}

		/* Creating a key value RDD using model name and weight of cars. */
		JavaPairRDD<String, String> wtData = carData
				.mapToPair(x -> new Tuple2<String, String>(x.split(",")[0], x.split(",")[5]));

		/* Printing Key Value data in wtRDD */
		System.out.println("Key Value Data = " + wtData.take(10));

		/* Printing Keys in wtRDD */
		wtData.keys().take(10).forEach(x -> System.out.println(x));

		/* Removing the table header */
		Tuple2<String, String> Top = wtData.first();
		JavaPairRDD<String, String> autoWT = wtData.filter(x -> !x.equals(Top));

		/* Printing Key Value Data in wtRDD */
		autoWT.take(10).forEach(x -> System.out.println(x));

		/*
		 * Mapping a count to the Values. Summing the weight of same models and
		 * counting them.
		 */
		JavaPairRDD<String, Tuple2<String, Integer>> kvRdd = autoWT.mapValues(x -> new Tuple2<String, Integer>(x, 1))
				.reduceByKey((x, y) -> new Tuple2<String, Integer>(
						String.valueOf(Integer.valueOf(x._1) + Integer.valueOf(y._1)), x._2 + y._2));

		/* Calculating and Printing Average Weight of each model. */
		kvRdd.collect().forEach(
				x -> System.out.printf("Model: %-40s Weight Average: %f \n", x._1, (Float.valueOf(x._2._1) / x._2._2)));

		/* Calculating average weight of each model to a KeyValue RDD. */
		JavaPairRDD<String, Float> avgWtRDD = kvRdd.mapValues(x -> Float.valueOf(x._1) / x._2);

		/* Printing Average Weight of each model. */
		avgWtRDD.collect().forEach(x -> System.out.println(x));

	}

}
