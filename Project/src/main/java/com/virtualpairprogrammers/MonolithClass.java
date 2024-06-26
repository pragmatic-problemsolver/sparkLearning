package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;

import java.util.ArrayList;
import java.util.List;

public class MonolithClass {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");//use the local configuration and * indicates to use all the cores available on the local machine
        //below one uses only one core
        //SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local");//use the local configuration and * indicates to use all the cores available on the local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1+value2);
        JavaRDD<Double> sqrtRdd = myRdd.map(value->Math.sqrt(value));

//        System.out.println(sqrtRdd); //this actually is a map and not the value by itself.

        sc.close();
    }
}
