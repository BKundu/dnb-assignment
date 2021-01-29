package com.sample.assignment.batch.reader;

import com.sample.assignment.batch.common.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class XMLReader extends Reader {
    String path;
    Map<String, String> metaDataMap;

    public XMLReader(SparkSession session, String path, Map<String, String> metaDataMap) {
        super.session = session;
        this.path = path;
        this.metaDataMap = metaDataMap;
    }

    public Dataset<Row> getDF() {
        Dataset<Row> df = session.read().format("com.databricks.spark.xml").option("inferSchema", "true")
                .option(Constants.ROW_TAG_META, metaDataMap.get(Constants.ROW_TAG_META)).load(path);
        return df;
    }
}
