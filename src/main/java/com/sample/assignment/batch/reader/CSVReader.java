package com.sample.assignment.batch.reader;

import com.sample.assignment.batch.common.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class CSVReader extends Reader {
    String path;
    Map<String, String> metaDataMap;

    public CSVReader(SparkSession session, String path, Map<String, String> metaDataMap) {
        super.session = session;
        this.path = path;
        this.metaDataMap = metaDataMap;
    }

    public Dataset<Row> getDF() {
        Dataset<Row> df = session.read().option("inferSchema", "true")
                .option(Constants.CSV_HEADER, metaDataMap.get(Constants.CSV_HEADER))
                .csv(path);
        return df;
    }
}
