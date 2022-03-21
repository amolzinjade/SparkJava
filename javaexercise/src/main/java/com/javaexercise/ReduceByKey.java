package com.javaexercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class ReduceByKey {
	public static void main(String args[])
	{
		List<String> inputdata = new ArrayList<>();
		inputdata.add("WARN: Tuesday");
		inputdata.add("ERROR: Tuesday");
		inputdata.add("FATAL: Wednesday");
		inputdata.add("ERROR: Friday");
		inputdata.add("WARN: Saturday");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("SPARK Transformations").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.parallelize(inputdata)
		 .mapToPair(rawValue->new Tuple2<>(rawValue.split(":")[0],1L))
		 .reduceByKey((value1,value2)->value1 + value2)
		 .foreach(tuple->System.out.println(tuple._1 + " has " +tuple._2 + " Instances"));

		sc.close();
	}


}
