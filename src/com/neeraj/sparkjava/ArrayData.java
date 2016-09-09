/**
 * ArrayData
 */
package com.neeraj.sparkjava;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class ArrayData. Example on how to create JavaRDD from an Array Data.
 * 
 * @author neeraj
 *
 */
public class ArrayData {

	/**
	 * Class ArrayData implements function 'callArrayData()' to create 'JavaRDD'
	 * from an Array Data.
	 */

	/**
	 * 'sparkContext' is used to create 'JavaRDD' from file data.
	 */
	private JavaSparkContext sparkContext = null;

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	ArrayData() {
		System.err.println("\nERROR: sparkContext is not initialized with a 'JavaSparkContext' in 'ArrayData'.\n"
				+ "Use parameterized constructor to initialize 'sparkContext'\n");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	ArrayData(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Creates a 'JavaRDD' from a random Array Data and performs various 'Spark
	 * Actions'.
	 */
	public void callArrayData() {

		/* Creating a List of Integers */
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 6, 3);

		/* Creating a JavaRDD of Integers using the List data */
		JavaRDD<Integer> rddData = sparkContext.parallelize(data);

		/*
		 * [OPTIONAL]Caching rddData to memory. To avoid repeated memory
		 * operation on each Spark Action Call. (Lazy Evaluation). Same as
		 * calling persist(MEMORY_ONLY).
		 */
		rddData.cache();

		/* Printing all the data in rddData */
		System.out.println("Data = " + rddData.collect());

		/* Printing distinct data in rddData */
		System.out.println("Distinct Data = " + rddData.distinct().collect());

		/* Printing first n=3 records in rddData */
		System.out.println("First Three Records = " + rddData.take(3));

		/* Printing first record in rddData */
		System.out.println("First Record = " + rddData.first());

		/* Printing the count of records in rddData */
		System.out.println("Count = " + rddData.count());

		/* Printing the count of distinct records in rddData */
		System.out.println("Distinct Count = " + rddData.distinct().count());

		/* Printing each record in rddData */
		rddData.collect().forEach(x -> System.out.println(x));

	}

}
