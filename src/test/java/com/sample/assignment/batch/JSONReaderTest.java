package com.sample.assignment.batch;

import com.sample.assignment.batch.common.Constants;
import com.sample.assignment.batch.reader.Reader;
import com.sample.assignment.batch.service.ReaderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JSONReaderTest {
    private static SparkSession sparkSession;
    private static ReaderFactory factory;
    private static Reader jsonReader;

    @BeforeClass
    public static void beforeClass() {
        sparkSession = SparkSession.builder().master("local[*]").config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(XMLReaderTest.class.getName()).getOrCreate();
        factory = new ReaderFactory();

        String jsonSourcePath = "src/test/resources/json/*.json";
        jsonReader = factory.getReader(Constants.JSON_SOURCE, jsonSourcePath, sparkSession, null);
    }

    @Test
    public void dfCountTest() {
        assertEquals(jsonReader.getDF().count(), 2);
    }

    @Test
    public void dfSchemaTest() {
        String[] dfColumns = jsonReader.getDF().columns().clone();
        String[] expectedColumns = {"activity"};
        Arrays.sort(dfColumns);
        Arrays.sort(expectedColumns);
        assertArrayEquals(dfColumns, expectedColumns);
    }

    @Test
    public void dfInnerSchemaTest() {
        String[] dfColumns = jsonReader.getDF().select("activity.userName", "activity.websiteName",
                "activity.activityTypeDescription", "activity.signedInTime").columns().clone();
        String[] expectedColumns = {"userName", "websiteName", "activityTypeDescription", "signedInTime"};
        Arrays.sort(dfColumns);
        Arrays.sort(expectedColumns);
        assertArrayEquals(dfColumns, expectedColumns);
    }

    @AfterClass
    public static void afterClass() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}