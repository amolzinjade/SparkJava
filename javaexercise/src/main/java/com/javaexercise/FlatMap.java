package com.javaexercise;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FlatMap {


	public static void main(String args[])
	{
		List<String> inputdata = new ArrayList<>();
		inputdata.add("WARN: Tuesday Septempber");
		inputdata.add("ERROR: Tuesday November");
		inputdata.add("FATAL: Wednesday May");
		inputdata.add("ERROR: Friday June");
		inputdata.add("WARN: Saturday July");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("SPARK Transformations").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> sentences = sc.parallelize(inputdata);
		JavaRDD<String> words = sentences.flatMap(value ->Arrays.asList(value.split(" ")).iterator());
		
		 for(String line:words.collect()){
	            System.out.println(" "+line);
	        }
	
		sc.close();
	}

}
