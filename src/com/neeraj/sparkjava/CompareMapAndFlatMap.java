/**
 * CompareMapAndFlatMap
 */
package com.neeraj.sparkjava;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class CompareMapAndFlatMap. Example to compare Spark Map Transformation and
 * Spark FlatMap Transformation.
 * 
 * @author neeraj
 *
 */
public class CompareMapAndFlatMap {
	/**
	 * Class CompareMapAndFlatMap implements function 'compare()' as a
	 * comparison between Spark Map Transformation and Spark FlatMap
	 * Transformation.
	 */

	/**
	 * 'sparkContext' is used to create 'JavaRDD' from file data. 'mapOut' is used to
	 * collect output of Spark Map Transformation.
	 */
	private JavaSparkContext sparkContext = null;
	private String mapOut = "";

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	CompareMapAndFlatMap() {
		System.err.println("\nERROR: sparkContext is not initialized with a 'JavaSparkContext' in 'CompareMapAndFlatMap'.\n"
				+ "Use parameterized constructor to initialize 'sparkContext'\n");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	CompareMapAndFlatMap(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Compares the working of Spark Map Transformation and Spark FlatMap
	 * Transformation on a same set of data.
	 */
	public void compare() {
		/* Creating a List of Strings. */
		List<String> data = Arrays.asList("My Name Is Neeraj", "Hi Neeraj", "Happy New Year");

		/* Creating a JavaRDD of String from List data. */
		JavaRDD<String> lines = sparkContext.parallelize(data).cache();

		/* Printing the content in List data */
		System.out.println("List of String: " + data);

		/* Printing the records in JavaRDD lines */
		System.out.println(lines.collect());
		/*
		 * Using Spark FlatMap Transformation and splitting each of the records
		 * in JavaRDD lines.
		 */
		JavaRDD<String> lineflatMap = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		/* Printing FlatMap Output */
		System.out.println("FlatMap Output: " + lineflatMap.collect());

		/*
		 * Using Spark Map Transformation and splitting each of the records in
		 * JavaRDD lines. Since Map returns only same no. of records, the record
		 * type is JavaRDD<String[]>.
		 */
		JavaRDD<String[]> lineMap = lines.map(s -> s.split(" "));

		/*
		 * Since output of map operation is a JavaRDD of String[], we need to
		 * collect each string from the String array
		 */
		lineMap.collect().forEach(x -> {
			mapOut = mapOut.concat("String Array: ");
			for (String i : x) {
				mapOut = mapOut.concat(i + " ");
			}
			mapOut = mapOut.concat("\n");
		});

		/* Printing Map Output */
		System.out.println("Map Output = \n" + mapOut);

	}

}
