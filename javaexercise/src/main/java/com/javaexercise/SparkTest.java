package com.javaexercise;

import org.apache.spark.sql.*;

public class SparkTest {

    private static final String CSV_URL="input.csv";

    public static void main(String args[]){
    	
    	System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.1");
        SparkSession spark=SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> csv = spark.read().format("csv")

                .option("sep", ",")

                .option("inferSchema", "true")

                .option("header", "true")

                .load(CSV_URL);

csv.show();

csv.printSchema();
    }

}