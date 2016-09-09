/**
 * SparkFlatMap
 */
package com.neeraj.sparkjava;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class SparkFlatMap. Example on how to use Spark FlatMap Transformation
 * 
 * @author neeraj
 *
 */
public class SparkFlatMap {
	/**
	 * Class SparkFlatMap implements function 'callFlatMap()' to describe Spark
	 * FlatMap Transformation.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create 'JavaRDD' from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	/**
	 * Default Constructor. Creates 'SparkSession' if SparkFlatMap object is
	 * created without 'SparkContext'.
	 */
	SparkFlatMap() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("Spark FlatMap Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	SparkFlatMap(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 */
	SparkFlatMap(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Performs Spark FlatMap Transformation.
	 */
	public void callFlatMap() {

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

		/* Filtering carData to make the data set smaller. */
		JavaRDD<String> audiData = carData.filter(s -> s.contains("audi"));

		/*
		 * FlatMap operation is performed on audiData by splitting each record
		 * on a String "Eu".
		 */
		JavaRDD<String> words = audiData.flatMap(s -> Arrays.asList(s.split("Eu")).iterator());

		/* Printing first 10 records in JavaRDD words */
		words.take(10).forEach(x -> System.out.println(x));

		/*
		 * FlatMap operation is performed on audiData by splitting each record
		 * on a String ",".
		 */
		JavaRDD<String> words1 = audiData.flatMap(s -> Arrays.asList(s.split(",")).iterator());

		/* Printing first 10 records in JavaRDD words1 */
		words1.take(10).forEach(x -> System.out.println(x));

		/* Printing total no.of records after each of the flatMap operation */
		System.out.println("Count 1: " + words.count() + "\nCount 2: " + words1.count());

	}

}
