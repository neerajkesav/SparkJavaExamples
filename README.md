## Spark Java Examples

This project is created to learn Apache Spark Programming using Java. This project consists of the following examples:

  * How to create SparkContext and SparkSession. 
  * Taking data from arrays and external file source. 
  * Spark Map Transformation. 
  * Spark Filter Transformation. 
  * Spark FlatMap Transformation. 
  * Compare Map and FlatMap. 
  * Set Operations. 
  * Spark Reduce Transformation. 
  * Spark Aggregate Transformation. 
  * Using Functions in Spark Transformation. 
  * Key Value RDD.
  * Using HDFS
 
### Data Sets
 * cars.csv - A data set with many attributes of various car models.	
 * Some random array data.		

### Getting Started

These instructions will get you a brief idea on setting up the environment and running on your local machine for development and testing purposes. 

**Prerequisities**

- Java
- Apache Spark 
- Hadoop

**Setup and running tests**

1. Run `javac` and `java -version` to check the installation
   
2. Run `spark-shell` and check if Spark is installed properly. 
 
3. Go to Hadoop user (If  installed on different user) and run the following (On Ubuntu Systems): 

      `sudo su hadoopuser`
          
      `start-all.sh`   
           
4. Execute the following commands from terminal to run the tests:

      `javac -classpath "Path to required jar files(spark, hadoop, scala)" Main.java` 

     

###Classes
Please start exploring from Main.java

All classes in this project are listed below:

* **CreateSpark.java** - To create SparkContext and SparkSession. Contains the following methods:
	
      	  `public JavaSparkContext context(String appName, String master)`
      	  `public SparkSession session(String appName, String master)`
	
* **ArrayData.java** - Using array data to create JavaRDD and performs spark actions on it. Contains the following method:
	
      	  `public void callArrayData()`
	
* **ExternalFileData.java** - Using external file source to create JavaRDD and performs spark actions on it. Contains the following method:

	  `public void callFileData(String filePath)`
	
* **SparkMap.java** - Example code on using Spark Map Transformation, contains the following method:
	 
      	  `public void mapReplace(String arg0, String arg1)`
	 
* **SparkFilter.java** - Example code on using Spark Filter Transformation, contains the following method:
	
	  `public void callFilter(String str)`
		
* **SparkFlatMap.java** - Example code on using Spark FlatMap Transformation, contains the following method:
	
	  `public void callFlatMap()`
	
* **CompareMapAndFlatMap.java** - To compare and understand Map and FlapMap Transformations. Contains the following method:
	
	  `public void compare()`
	
* **SetOperations.java** - Performing set operations on JavaRDD. Contains the following method: 
	
	  `public void callSetOp()`
	 
* **Reduce.java** - Examples on Spark Reduce Transformation. Contains the following methods:
	
	  `public void sum()`	
	  `public void shortestLine()`

* **Aggregation.java** - Uses two different use cases of Spark Aggregate Transformation. Contains the following methods:
	
	  `public void sum()`	
	  `public void sumAndProduct()` 
	
* **Functions.java** - Using Functions in Spark Transformation. Contains the following methods:
	
	  `public static void example1(JavaSparkContext sparkContext)`
	  `public static void example2(JavaSparkContext sparkContext)`
	

* **KeyValueRDD.java** - Examples on using Key Value RDD. Contains the following method:
	
	  `public void callKVRDD()`

* **UsingHDFS.java** - Example on using HDFS in Spark Programming. Contains the following methods:
	
	  `public <T> void saveToHDFS(JavaRDD<T> hdfsData, String path)` 
	  `public JavaRDD<String> readHDFS(String filePath)`
	
* **Main.java** - Main class to test and run the classes in this project.	






