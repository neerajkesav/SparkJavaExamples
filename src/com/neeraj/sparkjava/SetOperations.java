/**
 * SetOperations
 */
package com.neeraj.sparkjava;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class SetOperations. Example on how to use Set Operations.
 * 
 * @author neeraj
 *
 */
public class SetOperations {
	/**
	 * Class SetOperations implements function 'callSetOp()' with various set
	 * operations.
	 */

	/**
	 * sparkContext is used to create JavaRDD from file data.
	 */
	private JavaSparkContext sparkContext = null;

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	SetOperations() {
		System.err.println("\nERROR: sparkContext is not initialized with a JavaSparkContext in SetOperations.\n"
				+ "Use parameterized constructor to initialize sparkContext\n");
	}

	/**
	 * To create class object and to assign JavaSparkContext to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of JavaSparkContext from calling method.
	 */
	SetOperations(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Performs various set operations on JavaRDD. (Union, Intersection).
	 */
	public void callSetOp() {

		/* Creating two JavaRDDs to perform set operations. */
		JavaRDD<String> words1 = sparkContext.parallelize(Arrays.asList("May", "June", "April", "March")).cache();
		JavaRDD<String> words2 = sparkContext.parallelize(Arrays.asList("June", "April", "July")).cache();

		/* Printing Result. */
		System.out.println("words1 : " + words1.collect() + "\nwords2 : " + words2.collect() // Printing JavaRDDs																								

				+ "\nUnion = " + words1.union(words2).collect() 							 // Union.

				+ "\nUnion and Distinct = " + words1.union(words2).distinct().collect() 	 // Union and Distinct																						

				+ "\nIntersection = " + words1.intersection(words2).collect()); 			 // Intersection.

	}

}
