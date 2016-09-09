/**
 * SparkFilter
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class SparkFilter. Example on how to use Spark Filter Transformation.
 * 
 * @author neeraj
 *
 */
public class SparkFilter {
	/**
	 * Class SparkFilter implements function 'callFilter()' to describe Spark
	 * Filter Transformation.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create 'JavaRDD' from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	/**
	 * Default Constructor. Creates 'SparkSession' if SparkFilter object is
	 * created without 'SparkContext'.
	 */
	SparkFilter() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("Spark Filter Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	SparkFilter(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 */
	SparkFilter(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Performs Spark Filter Transformation
	 * 
	 * @param str
	 */
	public void callFilter(String str) {

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

		/* Filtering JavaRDD carData on String str. */
		JavaRDD<String> filteredData = carData.filter(s -> s.contains(str));

		/* Printing Filtered Data */
		filteredData.collect().forEach(x -> System.out.println("Filtered Data: " + x));

	}

}
