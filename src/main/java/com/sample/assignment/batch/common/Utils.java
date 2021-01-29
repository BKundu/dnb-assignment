package com.sample.assignment.batch.common;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Set;

public class Utils {
    public static SparkSession getSparkSession(Map<String, String> propMap) {

        SparkConf conf = new SparkConf();
        Set<String> allProps = propMap.keySet();

        //Assumed that all the properties in config file starting with spark. will be used for SparkConf
        for (String prop : allProps) {
            if (prop.startsWith("spark.")) {
                conf.set(prop, propMap.get(prop));
            }
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        return sparkSession;
    }
}
