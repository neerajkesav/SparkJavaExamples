/**
 * Functions
 */
package com.neeraj.sparkjava;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Class Functions. Examples on how to use functions on Spark Transformations.
 * 
 * @author neeraj
 *
 */
public class Functions {

	/**
	 * Class Functions implements two functions 'example1()' and 'example2()' to
	 * describe the use of functions on Spark Transformations.
	 * 
	 */

	/**
	 * Function 'example1()' uses a 'JavaRDD' of car details and performs cleanse and
	 * transform operations on it.
	 * 
	 * @param sparkContext
	 *            Takes an instance of 'JavaSparkContext' from the calling method.
	 */
	public static void example1(JavaSparkContext sparkContext) {

		/* Creating JavaRDD of String and caching to memory */
		JavaRDD<String> carData = sparkContext.textFile("/home/neeraj/cars.csv").cache();

		/* Filtering carData which contains CharSequence "audi" */
		JavaRDD<String> audiData = carData.filter(s -> s.contains("audi"));

		/**
		 * Class MyFunction implements function 'call()' for cleansing the data.
		 * 
		 * @author neeraj
		 *
		 */
		@SuppressWarnings("serial")
		class MyFunction implements Function<String, String> {

			/**
			 * Function 'call()' changes the no. of cylinders to text.
			 * 
			 * @param s
			 *            Takes each of the records from the JavaRDD.
			 */
			public String call(String record) {
				/* Splitting the String record to attributes. */
				String[] attList = record.split(",");
				if (attList[2].equals("4")) {
					attList[2] = "FOUR";
				} else if (attList[2].equals("6")) {
					attList[2] = "SIX";
				} else {
					attList[2] = "EIGHT";
				}

				/* Changing the Origin of car model to upper case. */
				attList[8] = attList[8].toUpperCase();

				/* Joining the attributes after cleansing */
				String str = String.join(",", attList);
				return str;
			}

		}

		/*
		 * MyFunction is called for cleansing operations using Spark Map
		 * Transformation.
		 */
		JavaRDD<String> cleansedData = audiData.map(new MyFunction());

		/* Printing the cleansed data */
		System.out.println("Cleansed Data =" + cleansedData.collect());
	}

	/**
	 * Function 'example2()' uses a 'JavaRDD' of car details and calculates average
	 * mileage of cars.
	 * 
	 * @param sparkContext
	 *            Takes an instance of JavaSparkContext from the calling method.
	 */
	public static void example2(JavaSparkContext sparkContext) {

		/* Creating JavaRDD of String and caching to memory */
		JavaRDD<String> carData = sparkContext.textFile("/home/neeraj/SWork/cars.csv").cache();

		/**
		 * Class MyFunction2 implements function 'call()' to get the mileage of
		 * each car.
		 *  
		 * @author neeraj
		 *
		 */
		@SuppressWarnings("serial")
		class MyFunction2 implements Function<String, String> {

			/**
			 * Function 'call()' returns the mileage of each car.
			 * 
			 * @param s
			 *            Takes each of the records from the JavaRDD.
			 */
			public String call(String record) {

				/* Splitting the String record to attributes. */
				String[] attList = record.split(",");

				if (attList.length > 1) {
					if (attList[1].equals("MPG"))	// Header text .
						return "0";
					return attList[1];				// Mileage.
				} else
					return record;						// Intermediate sum of mileage.
			}
		}

		/* Creating MyFunction2 object. */
		MyFunction2 myFunc = new MyFunction2();

		/*
		 * Calculating Average Mileage. (Reduce operation returns the sum of
		 * mileage.)
		 */
		float average = Float
				.valueOf(carData.reduce((x, y) -> String.valueOf(Float.valueOf(myFunc.call(x)) + Float.valueOf(myFunc.call(y)))))
				/ (-1 + carData.count());

		/* Printing Average. */
		System.out.println("Average =" + average);

	}

}
