package com.sample.assignment.batch;

import com.sample.assignment.batch.common.ApplicationPropertyLoader;
import com.sample.assignment.batch.common.Constants;
import com.sample.assignment.batch.common.Utils;
import com.sample.assignment.batch.reader.Reader;
import com.sample.assignment.batch.service.ReaderFactory;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;

public class DriverMain {

    static final Logger logger = Logger.getLogger(DriverMain.class);

    public static void main(String[] args) {
        //Properties file path can also be passed as java system properties. I am following the args option.
        //String propFilePath = "./src/main/resources/config/config.properties";
        String propFilePath = args[0];
        try {
            logger.info("Loading application properties");
            ApplicationPropertyLoader propLoader = ApplicationPropertyLoader.getInstance();
            Map<String, String> propMap = propLoader.getpropertiesMap(propFilePath);
            logger.info("Application properties loaded successfully");

            logger.info("Requesting for Spark session");
            SparkSession sparkSession = Utils.getSparkSession(propMap);
            logger.info("Spark session created successfully");

            ReaderFactory factory = new ReaderFactory();

            logger.info("Requesting for an instance of JSONReader");
            Reader jsonReader = factory.getReader(Constants.JSON_SOURCE, propMap.get(Constants.JSON_SOURCE_PROP_KEY),
                    sparkSession, null);
            Dataset<Row> jsonDF = jsonReader.getDF();

            //In case of JSON df transformation and projections have been done using Dataframe APIs
            Dataset<Row> jsonDFWithRequiredColumns = jsonDF.select("activity.userName", "activity.websiteName",
                    "activity.activityTypeDescription", "activity.signedInTime");
            Dataset<Row> jsonDFRenamedColumns = jsonDFWithRequiredColumns.withColumnRenamed("userName", "user")
                    .withColumnRenamed("websiteName", "website");
            Dataset<Row> jsonDFOutput = jsonDFRenamedColumns.select(jsonDFRenamedColumns.col("user"),
                    jsonDFRenamedColumns.col("website"),
                    jsonDFRenamedColumns.col("activityTypeDescription"),
                    functions.to_timestamp(functions.to_date(jsonDFRenamedColumns.col("signedInTime"), "MM/dd/yyyy"), "yyyy-MM-dd hh:mm:ss").as("signedInTime"));


            Map<String, String> metadataMapXML = new HashMap<>();
            metadataMapXML.put(Constants.ROW_TAG_META, "activity");
            logger.info("Requesting for an instance of XMLReader");
            Reader xmlReader = factory.getReader(Constants.XML_SOURCE, propMap.get(Constants.XML_SOURCE_PROP_KEY), sparkSession, metadataMapXML);

            Map<String, String> metadataMapCSV = new HashMap<>();
            metadataMapCSV.put(Constants.CSV_HEADER, "true");
            logger.info("Requesting for an instance of CSVReader");
            Reader csvReader = factory.getReader(Constants.CSV_SOURCE, propMap.get(Constants.CSV_SOURCE_PROP_KEY), sparkSession, metadataMapCSV);

            Dataset<Row> xmlDF = xmlReader.getDF();
            Dataset<Row> csvDF = csvReader.getDF();

            xmlDF.createOrReplaceTempView("xmlDF");
            csvDF.createOrReplaceTempView("csvDF");

            //In this case transformation and projections have been done using spark sql.
            // This is to demonstrate, I'm comfortable in both API as well as SQL
            //Used left outer join to ensure we don't miss any activity_code which is not available in lookup
            logger.info("Performing Spark SQL join to lookup activity description");
            Dataset<Row> xmlDFOutput = sparkSession.sql("select " +
                    "x.userName as user, " +
                    "x.websiteName as website, " +
                    "coalesce(c.activity_description, x.activityTypeCode) as activityDescription, " +
                    "to_timestamp(x.loggedInTime, 'yyyy-MM-dd') as signedInTime " +
                    "from xmlDF x left join csvDF c on x.activityTypeCode=c.activity_code");

            Dataset<Row> finalOutput = jsonDFOutput.unionAll(xmlDFOutput);

            logger.info("Writing resultant dataset to output path");
            finalOutput.write().mode(SaveMode.Overwrite).json(propMap.get(Constants.APP_OUTPUT_DIR_KEY));
            logger.info("Application completed successfully");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Exception while processing " + e.getMessage());
            System.exit(-1);
        }
    }
}
