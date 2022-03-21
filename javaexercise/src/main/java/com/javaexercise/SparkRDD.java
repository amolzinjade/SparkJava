package com.javaexercise;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
public class SparkRDD 
{
	private static final String CSV_URL="input.csv";
	public static void main(String args[])
	{
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("RDD Example").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(CSV_URL);
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		JavaRDD<Integer> mapData = lines.map(x->x.length());
		JavaRDD<String> flatmapData = lines.flatMap(s -> Arrays.asList(s.split(",")).iterator());
		
		System.out.println("=================Map Transformation============"); 
		for(int line:mapData.collect()){
	            System.out.println(" "+line);
	        }
		int totalLength = mapData.reduce((a, b) -> a + b);
		System.out.println(totalLength);
		 
		System.out.println("=================FlatMap Transformation============");
		 for(String line:flatmapData.collect()){
	            System.out.println(" "+line);
	        }
		 
		 
	}
}
