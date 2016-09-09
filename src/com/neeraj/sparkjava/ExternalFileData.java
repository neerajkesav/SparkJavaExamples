/**
 * ExternalFileData
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class ExternalFileData. Example on how to create 'JavaRDD' from an external
 * file source.
 * 
 * @author neeraj
 *
 */
public class ExternalFileData {
	/**
	 * Class ExternalFileData implements function 'callFileData()' to create
	 * 'JavaRDD' from an external file source.
	 */

	/**
	 * Either of 'sparkContext' and 'sparkSession' is used to create 'JavaRDD' from
	 * file data.
	 */
	private JavaSparkContext sparkContext = null;
	private SparkSession sparkSession = null;

	/**
	 * Default Constructor. Creates 'SparkSession' if 'ExternalFileData' object is
	 * created without 'SparkContext'.
	 */
	ExternalFileData() {
		/* Creates CreateSpark object */
		CreateSpark spark = new CreateSpark();

		/* Creating SparkSession */
		this.sparkSession = spark.session("External File Data Sample", "local");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	ExternalFileData(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * To create class object and to assign 'SparkSession' to class variable.
	 * 
	 * @param sparkSession
	 *            contains the instance of 'SparkSession' from calling method.
	 */
	ExternalFileData(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Creates a 'JavaRDD' from an external file source and performs various 'Spark
	 * Actions'.
	 */
	public void callFileData(String filePath) {

		/* Creating JavaRDD of String. */
		JavaRDD<String> carData;

		if (sparkContext == null) {// Checking sparkContext is null.

			/*
			 * Assign records from cars.csv to carData using sparkSession and
			 * caching to memory.
			 */
			carData = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD().cache();

		} else {

			/*
			 * Assign records from cars.csv to carData using sparkContext and
			 * caching to memory.
			 */
			carData = sparkContext.textFile(filePath).cache();

		}

		/* Printing all the data in carData */
		System.out.println("Data = " + carData.collect());

		/* Printing first n=3 records in carData */
		System.out.println("First Three Records = " + carData.take(3));

		/* Printing first record in carData */
		System.out.println("First Record = " + carData.first());

		/* Printing the count of records in carData */
		System.out.println("Count = " + carData.count());

		/* Printing each record in carData */
		carData.collect().forEach(x -> System.out.println(x));

	}

}
