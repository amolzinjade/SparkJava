package com.javaexercise;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TestingJoins {
	
	public static void main(String args[])
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("SPARK Transformations").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer,Integer>> visitsRow = new ArrayList<>();
		visitsRow.add(new Tuple2<>(4,18));
		visitsRow.add(new Tuple2<>(6,4));
		visitsRow.add(new Tuple2<>(10,9));
		
		List<Tuple2<Integer,String>> userData = new ArrayList<>();
		userData.add(new Tuple2<>(1,"John"));
		userData.add(new Tuple2<>(2,"Bob"));
		userData.add(new Tuple2<>(3,"Alan"));
		userData.add(new Tuple2<>(4,"Doris"));
		userData.add(new Tuple2<>(5,"Marybelle"));
		userData.add(new Tuple2<>(6,"Raquel"));
		
		JavaPairRDD<Integer,Integer> visits = sc.parallelizePairs(visitsRow);
		JavaPairRDD<Integer,String> users = sc.parallelizePairs(userData);
		
		//Below code is for inner join
		System.out.println("=========Inner Join===========");
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
		for(Tuple2<Integer, Tuple2<Integer, String>> line:joinedRdd.collect())
		{
			System.out.println(line);
		}
		System.out.println();
		//Below code is for Left outer join
		System.out.println("=========Left Outer Join===========");
		
		JavaPairRDD<Integer, Tuple2<Integer, org.apache.spark.api.java.Optional<String>>> leftjoinedRdd = visits.leftOuterJoin(users);
		for(Tuple2<Integer, Tuple2<Integer, org.apache.spark.api.java.Optional<String>>> line:leftjoinedRdd.collect())
		{
			System.out.println(line);
		}
		System.out.println();
		//Below code is for Left outer join
		System.out.println("=========Right Outer Join===========");
		
		JavaPairRDD<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, String>> rightjoinedRdd = visits.rightOuterJoin(users);
		for(Tuple2<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, String>> line:rightjoinedRdd.collect())
		{
			System.out.println(line);
		}
		System.out.println();
		//Below code is for Full outer join
		System.out.println("=========Full Outer Join===========");
		
		JavaPairRDD<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<String>>> fullOuterjoin = visits.fullOuterJoin(users);
		for(Tuple2<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<String>>> line:fullOuterjoin.collect())
		{
			System.out.println(line);
		}			
	
		System.out.println();
		//Below code is for cartesian join
		System.out.println("=========Cartesian Join===========");
		
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoin = visits.cartesian(users);
		for(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>> line:cartesianJoin.collect())
		{
			System.out.println(line);
		}			
		sc.close();
	}

}
