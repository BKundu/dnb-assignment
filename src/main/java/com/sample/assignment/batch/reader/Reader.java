package com.sample.assignment.batch.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Reader {
    public SparkSession session;

    public abstract Dataset<Row> getDF();
}
