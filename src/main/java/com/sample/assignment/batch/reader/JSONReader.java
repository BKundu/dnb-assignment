package com.sample.assignment.batch.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONReader extends Reader {
    String path;

    public JSONReader(SparkSession session, String path) {
        super.session = session;
        this.path = path;
    }

    public Dataset<Row> getDF() {
        Dataset<Row> df = session.read().json(path);
        return df;
    }
}
