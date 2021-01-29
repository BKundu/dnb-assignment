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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class XMLReaderTest {
    private static SparkSession sparkSession;
    private static ReaderFactory factory;
    private static Reader xmlReader;

    @BeforeClass
    public static void beforeClass() {
        sparkSession = SparkSession.builder().master("local[*]").config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(XMLReaderTest.class.getName()).getOrCreate();
        factory = new ReaderFactory();

        String xmlSourcePath = "src/test/resources/xml/*.xml";
        Map<String, String> metadataMapXML = new HashMap();
        metadataMapXML.put(Constants.ROW_TAG_META, "activity");
        xmlReader = factory.getReader(Constants.XML_SOURCE, xmlSourcePath, sparkSession, metadataMapXML);
    }

    @Test
    public void dfCountTest() {
        assertEquals(xmlReader.getDF().count(), 2);
    }

    @Test
    public void dfSchemaTest() {
        String[] dfColumns = xmlReader.getDF().columns().clone();
        String[] expectedColumns = {"userName", "websiteName", "activityTypeCode", "loggedInTime", "number_of_views"};
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