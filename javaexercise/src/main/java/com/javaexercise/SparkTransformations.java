package com.javaexercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkTransformations {

	public static void main(String args[])
	{
		List<Integer> inputdata = new ArrayList<>();
		inputdata.add(35);
		inputdata.add(12);
		inputdata.add(90);
		inputdata.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("SPARK Transformations").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRdd = sc.parallelize(inputdata);
		
		//Reduce function to calculate addition using reduce
		Integer result = myRdd.reduce((value1,value2)->value1 + value2);
		System.out.println(result);
		
		//Map function to calculate square root
		JavaRDD<Double> sqrtRdd = myRdd.map(value->Math.sqrt(value));
		sqrtRdd.foreach(value -> System.out.println(value));
		
		
		sc.close();
	}
}
