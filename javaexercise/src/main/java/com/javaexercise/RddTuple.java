package com.javaexercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RddTuple {

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
		JavaRDD<Tuple2<Integer,Double>> sqrtRdd = myRdd.map(value-> new Tuple2(value,Math.sqrt(value)));
		Tuple2<Integer,Double> myValue = new Tuple2(9,3);
		
		sc.close();
	}
}
