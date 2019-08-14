package com.muni;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;

/**
 * Created by muni on 2019/8/14
 */
public class SparkMain {
    public static void main (String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.master", "local")
                .appName("Simple Application").getOrCreate();

        String logFile = SparkMain.class.getClassLoader().getResource("test.txt").getPath();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        Dataset<Row> flatMap = logData.flatMap((FlatMapFunction<String, String>) s ->
                Arrays.asList(s.split(" ")).iterator(), Encoders.STRING()).toDF("word");
        flatMap.show(10);

        Dataset<Row> dataSet = flatMap
                .groupBy("word")
                .count()
                .toDF("word", "count")
                .sort(functions.desc("count"));
        dataSet.show(10);

        spark.stop();
    }
}
