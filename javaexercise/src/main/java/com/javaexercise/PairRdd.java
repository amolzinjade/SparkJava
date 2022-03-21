package com.javaexercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRdd {


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
		
		JavaRDD<String> logMessages = sc.parallelize(inputdata);
		
		JavaPairRDD<String, Long> pairRDD = logMessages.mapToPair(rawValue->{
			String[] columns = rawValue.split(":");
			String level = columns[0];
			String day = columns[1];
			return new Tuple2<>(level, 1L);
		});
			
		JavaPairRDD<String, Long> sumRdd = pairRDD.reduceByKey((value1,value2)->value1+value2);
		sumRdd.foreach(tuple->System.out.println(tuple._1 + " has " +tuple._2 + " Instances"));
		sc.close();
	}

}
