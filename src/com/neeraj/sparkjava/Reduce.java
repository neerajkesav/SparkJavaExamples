/**
 * Reduce
 */
package com.neeraj.sparkjava;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class Reduce. Example on how to use Spark Reduce Transformation.
 * 
 * @author neeraj
 *
 */
public class Reduce {

	/**
	 * Class Reduce implements functions 'sum()' and 'shortestLine()' to
	 * describe the usage of Spark Reduce Transformation.
	 */

	/**
	 * 'sparkContext' is used to create 'JavaRDD' from file data.
	 */
	private JavaSparkContext sparkContext = null;

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	Reduce() {
		System.err.println("\nERROR: sparkContext is not initialized with a JavaSparkContext in Reduce.\n"
				+ "Use parameterized constructor to initialize sparkContext\n");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	Reduce(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Calculating Sum using Spark Reduce Transformation.
	 */
	public void sum() {

		/* Creating a JavaRDD of Integers using a random array. */
		JavaRDD<Integer> rddData = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 6, 3)).cache();

		/* Calculating Sum. */
		int sum = rddData.reduce((x, y) -> x + y);

		/* Printing array data and sum. */
		System.out.println("Data: " + rddData.collect() + "\nSum using Reduce Op = " + sum);

		/*
		 * [OPTIONAL]: Removing rddData from memory. Spark will do this
		 * automatically.
		 */
		rddData.unpersist();

	}

	/**
	 * Finding the shortest line using Spark Reduce Transformation.
	 */
	public void shortestLine() {

		/* Creating JavaRDD of String and caching to memory. */
		JavaRDD<String> carData = sparkContext.textFile("/home/neeraj/cars.csv").cache();

		/* Finding the shortest line. */
		String shortest = carData.reduce((x, y) -> {
			if (x.length() <= y.length())
				return x;
			else
				return y;
		});

		/* Printing the file content and shortest line from the file. */
		System.out.println("File Content:\n" + carData.collect() + "\n\nShortest Line = " + shortest);

		/*
		 * [OPTIONAL]: Removing carData from memory. Spark will do this
		 * automatically.
		 */
		carData.unpersist();

	}
}
