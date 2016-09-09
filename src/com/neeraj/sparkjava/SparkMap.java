/**
 * SparkMap
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class SparkMap. Example on how to use Spark Map Transformation.
 * 
 * @author neeraj
 *
 */
public class SparkMap {
	/**
	 * Class SparkMap implements a function 'mapReplace()' to describe Spark Map
	 * Transformation.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create 'JavaRDD' from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	/**
	 * Default Constructor. Creates 'SparkSession' if SparkMap object is created
	 * without 'SparkContext'.
	 */
	SparkMap() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("Spark Map Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	SparkMap(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 */
	SparkMap(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Performs Spark Map Transformation with Replace operation on each record.
	 * 
	 * @param arg0
	 *            Takes the CharSequence to replace with.
	 * @param arg1
	 *            Takes the new CharSequence to replace arg0.
	 */
	public void mapReplace(String arg0, String arg1) {

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

		/*
		 * Performing Map by replacing CharSequence arg0 with CharSequence arg1
		 * in each record.
		 */
		JavaRDD<String> mapData = carData.map(s -> s.replace(arg0, arg1));

		/* Printing resulting JavaRDD after Map transformattion. */
		System.out.println("Data after replace:");
		mapData.collect().forEach(x -> System.out.println(x));

	}

}
