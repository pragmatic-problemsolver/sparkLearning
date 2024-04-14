package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class MonolithClass {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(1.0);
        inputData.add(2.0);
        inputData.add(3.0);
        inputData.add(4.0);
        inputData.add(5.0);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");//use the local configuration and * indicates to use all the cores available on the local machine
        //below one uses only one core
        //SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local");//use the local configuration and * indicates to use all the cores available on the local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        Double result = myRdd.reduce((value1, value2) -> value1+value2);
        System.out.println(result);
        sc.close();
    }
}
