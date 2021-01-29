package com.sample.assignment.batch.service;

import com.sample.assignment.batch.common.ApplicationPropertyLoader;
import com.sample.assignment.batch.common.Constants;
import com.sample.assignment.batch.reader.CSVReader;
import com.sample.assignment.batch.reader.JSONReader;
import com.sample.assignment.batch.reader.Reader;
import com.sample.assignment.batch.reader.XMLReader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class ReaderFactory {
    static final Logger logger = Logger.getLogger(ReaderFactory.class);

    public Reader getReader(String sourceType, String path, SparkSession sparkSession, Map<String, String> metadataMap) {
        if (sourceType == null) {
            logger.warn("Received null source type, can not create an instance of Reader");
            return null;
        }
        if (sourceType.equalsIgnoreCase(Constants.JSON_SOURCE)) {
            return new JSONReader(sparkSession, path);
        } else if (sourceType.equalsIgnoreCase(Constants.XML_SOURCE)) {
            return new XMLReader(sparkSession, path, metadataMap);
        } else if (sourceType.equalsIgnoreCase(Constants.CSV_SOURCE)) {
            return new CSVReader(sparkSession, path, metadataMap);
        }
        logger.warn("Received unknown source type, can not create an instance of Reader");
        return null;
    }
}