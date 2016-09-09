/**
 * Aggregation
 */
package com.neeraj.sparkjava;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class Aggregation. Example on how to use Spark Aggregate Transformation
 * 
 * @author neeraj
 *
 */
public class Aggregation {
	/**
	 * Class Aggregation implements two functions 'sum()' and 'sumAndProduct()'.
	 * Describes two different use cases of Spark Aggregate Transformation.
	 */

	/**
	 * 'sparkContext' is used to create 'JavaRDD' from file data.
	 */
	private JavaSparkContext sparkContext = null;

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	Aggregation() {
		System.err.println("\nERROR: sparkContext is not initialized with a 'JavaSparkContext' in 'Aggregation'.\n"
				+ "Use parameterized constructor to initialize 'sparkContext'\n");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	Aggregation(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Calculates Sum using Spark Aggregate Transformation.
	 */
	public void sum() {

		/*
		 * Creating JavaRDD of type Integer from a random integer data and
		 * caching to memory.
		 */
		JavaRDD<Integer> rddData = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 6, 3)).cache();

		/* Calculating Sum using Spark Aggregate Transformation */
		int sum = rddData.aggregate(0, (x, y) -> {
			return x + y;
		}, (x, y) -> {
			return x + y;
		});

		/* Printing rddData and Sum */
		System.out.println("Data: " + rddData.collect() + "\nSum using Aggregate Op = " + sum);

		/*
		 * [OPTIONAL]: Removing rddData from memory. Spark will do this
		 * automatically.
		 */
		rddData.unpersist();
	}

	/**
	 * Calculate Sum and Product together using Spark Aggregate Transformation.
	 */
	public void sumAndProduct() {

		/*
		 * Creating JavaRDD of type Integer from a random integer data and
		 * caching to memory.
		 */
		JavaRDD<Integer> rddData = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 6, 3)).cache();

		/*
		 * Addition and multiplication at the same time SeqOP:x is a tuple of
		 * sequence. CombOp: x and y are tuples. Calculate Sum and Product using
		 * Spark Aggregate Transformation.
		 */
		List<Integer> result = rddData.aggregate(Arrays.asList(0, 1), (x, y) -> {
			return Arrays.asList(x.get(0) + y, x.get(1) * y);
		}, (x, y) -> {
			return Arrays.asList(x.get(0) + y.get(0), x.get(1) * y.get(1));
		});

		/* Printing rddData and Result */
		System.out.println("Data: " + rddData.collect() + "\nSum and Product using Aggregate Op = " + result);

		/*
		 * [OPTIONAL]: Removing rddData from memory. Spark will do this
		 * automatically.
		 */
		rddData.unpersist();

	}

}
