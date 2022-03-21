package com.javaexercise;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.shell.Command;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ViewingFigures {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
		SparkConf conf = new SparkConf().setAppName("SPARK Big Data Exercise").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//set this to use data from hardcoded data
		boolean testMode = false;

		JavaPairRDD<Integer,Integer> viewData = setViewDataRdd(sc,testMode);
		JavaPairRDD<Integer,Integer> chapterData = setChapterDataRdd(sc,testMode);
		JavaPairRDD<Integer,String> titlesData = setTitlesDataRdd(sc,testMode);
	
		// Warmup
				JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair( row -> new Tuple2<Integer, Integer>(row._2, 1))
				           .reduceByKey( (value1, value2) -> value1 + value2);

				
				// Step 1 - remove any duplicated views
				viewData = viewData.distinct();
				
				// Step 2 - get the course Ids into the RDD
				viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1 ));
				JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData);
				
				// Step 3 - don't need chapterIds, setting up for a reduce
				JavaPairRDD<Tuple2<Integer, Integer>, Long > step3 = joinedRdd.mapToPair( row -> {
					Integer userId = row._2._1;
					Integer courseId = row._2._2;
					return new Tuple2<Tuple2<Integer, Integer>, Long>( new Tuple2<Integer, Integer>(userId, courseId), 1L);
				});
				
				
				// Step 4 - count how many views for each user per course
				step3 = step3.reduceByKey((value1, value2) -> value1+value2);
				
				// step 5 - remove the userIds
				JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));
				
				// step 6 - add in the total chapter count
				JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRdd);
				
				// step 7 - convert to percentage
				JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double)value._1 / value._2);
				
				// step 8 - convert to scores
				JavaPairRDD<Integer, Long> step8 = step7.mapValues(value -> {
					if (value > 0.9) return 10L;
					if (value > 0.5) return 4L;
					if (value > 0.25) return 2L;
					return 0L;
				});
				
				// step 9
				step8 = step8.reduceByKey((value1, value2) -> value1+value2);
				
				// step 10
				JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step8.join(titlesData);
				
				JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<Long, String>(row._2._1, row._2._2));
				step11.sortByKey(false).collect().forEach(System.out::println);
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode)
		{
			List<Tuple2<Integer,String>> rawTitle = new ArrayList<>();
			rawTitle.add(new Tuple2<>(1,"How to find the better job"));
			rawTitle.add(new Tuple2<>(2,"Hard faster border smarter will you drep"));
			rawTitle.add(new Tuple2<>(3,"Content creation is big game"));
			return sc.parallelizePairs(rawTitle);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				.mapToPair(comma->{
					String[] cols = comma.split(",");
					return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				});
	}

	private static JavaPairRDD<Integer, Integer> setChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode)
		{
			List<Tuple2<Integer,Integer>> rawChapters = new ArrayList<>();
			rawChapters.add(new Tuple2<>(96,1));
			rawChapters.add(new Tuple2<>(97,1));
			rawChapters.add(new Tuple2<>(98,1));
			rawChapters.add(new Tuple2<>(99,2));
			rawChapters.add(new Tuple2<>(100,3));
			rawChapters.add(new Tuple2<>(101,3));
			rawChapters.add(new Tuple2<>(102,3));
			rawChapters.add(new Tuple2<>(103,3));
			rawChapters.add(new Tuple2<>(104,3));
			rawChapters.add(new Tuple2<>(105,3));
			rawChapters.add(new Tuple2<>(106,3));
			rawChapters.add(new Tuple2<>(107,3));
			rawChapters.add(new Tuple2<>(108,3));
			rawChapters.add(new Tuple2<>(109,3));
			
			return sc.parallelizePairs(rawChapters);
		}
		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
				.mapToPair(comma->{
					String[] cols = comma.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]),new Integer(cols[1]));
				});
	}

	private static JavaPairRDD<Integer, Integer> setViewDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode)
		{
			// Chapter views - (userId, chapterId)
						List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
						rawViewData.add(new Tuple2<>(14, 96));
						rawViewData.add(new Tuple2<>(14, 97));
						rawViewData.add(new Tuple2<>(13, 96));
						rawViewData.add(new Tuple2<>(13, 96));
						rawViewData.add(new Tuple2<>(13, 96));
						rawViewData.add(new Tuple2<>(14, 99));
						rawViewData.add(new Tuple2<>(13, 100));
						return  sc.parallelizePairs(rawViewData);
		}
		return sc.textFile("src/main/resources/viewing figures/views-1.csv")
				.mapToPair(comma->{
					String[] cols = comma.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]),new Integer(cols[1]));
				});
	}

}
