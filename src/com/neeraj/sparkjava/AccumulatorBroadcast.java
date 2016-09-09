/**
 * AccumulatorBroadcast
 */
package com.neeraj.sparkjava;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 * Class AccumulatorBroadcast. Example on how to use 'Accumulator and Broadcast'
 * variables.
 * 
 * @author neeraj
 */
@SuppressWarnings("deprecation")
public class AccumulatorBroadcast {

	/**
	 * Class AccumulatorBroadcast implements a function 'call()' to describe the
	 * usage of 'Accumulator' and 'Broadcast' variables.
	 * 
	 */

	/**
	 * 'sparkContext' is used to create 'JavaRDD' from file data.
	 */
	private JavaSparkContext sparkContext = null;

	/**
	 * Prints error message if class object is created using default
	 * constructor.
	 */
	AccumulatorBroadcast() {
		System.err.println("\nERROR: sparkContext is not initialized with a 'JavaSparkContext' in 'AccumulatorBroadcast'.\n"
				+ "Use parameterized constructor to initialize 'sparkContext'\n");
	}

	/**
	 * To create class object and to assign 'JavaSparkContext' to class variable.
	 * 
	 * @param sparkContext
	 *            contains the instance of 'JavaSparkContext' from calling method.
	 */
	AccumulatorBroadcast(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Describes how to use 'Accumulator' and 'Broadcast' variables. Counting car
	 * models according to their origin.
	 * 
	 */

	public void call() {

		/* Setting and Initializing Accumulator Variables. */
		Accumulator<Integer> accUS = sparkContext.accumulator(0);
		Accumulator<Integer> accEuro = sparkContext.accumulator(0);
		Accumulator<Integer> accJapan = sparkContext.accumulator(0);

		/* Setting and Initializing Broadcast Variables */
		Broadcast<String> bcvUS = sparkContext.broadcast("American");
		Broadcast<String> bcvEuro = sparkContext.broadcast("European");
		Broadcast<String> bcvJapan = sparkContext.broadcast("Japanese");

		/* Creating JavaRDD of String from cars.csv and caching to memory. */
		JavaRDD<String> carData = sparkContext.textFile("/home/neeraj/cars.csv").cache();

		/**
		 * Class CountOrigin is consists of a function 'call()' which counts the
		 * car models.
		 * 
		 * @author neeraj
		 */
		@SuppressWarnings("serial")
		class CountOrigin implements Function<String, String> {

			/**
			 * Broadcast variable is used to verify the origin of each car model
			 * and accumulator variables is used to count the car models.
			 */

			/**
			 * Function 'call()' counts the car models according to their origin
			 * 
			 * @param s
			 *            single record from JavaRDD carData
			 */
			public String call(String record) {

				/* Splitting string record */
				String[] attList = record.split(",");

				if (attList[8].equals(bcvUS.getValue())) { // Origin: American
					accUS.add(1);
				} else if (record.split(",")[8].equals(bcvEuro.getValue())) { // Origin:European					
					accEuro.add(1);
				} else if (record.split(",")[8].equals(bcvJapan.getValue())) {// Origin:Japanese					
					accJapan.add(1);
				}
				return record;
			}
		}

		/* Creating CountOrgin object */
		CountOrigin countOrigin = new CountOrigin();

		/*
		 * Calling map transformation on cardData. For each record in carData
		 * function call() of CountOrigin is called. After map transformation
		 * each of the Accumulator variable will contain the count for
		 * respective origins. Variable count contains the count of total no. of
		 * records in carData.
		 */
		long count = carData.map(x -> countOrigin.call(x)).count();

		/* Printing the results */
		System.out.println("Total Count: " + count + "\n US: " + accUS.value() + " Europe: " + accEuro.value()
				+ " Japan: " + accJapan.value());

	}

}
