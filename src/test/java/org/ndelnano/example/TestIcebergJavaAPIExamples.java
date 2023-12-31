package org.ndelnano.example;

import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/*
This class assumes that the following services are accessible with the configuration in org.ndelnano.example.Util.getConfiguration
REST Catalog
Minio
Spark
 */

public class TestIcebergJavaAPIExamples {

    static RESTCatalog catalog;
    static Configuration conf;
    static SparkSession spark;
    static String CATALOG_NAME = "iceberg";
    static String SCHEMA_NAME = "demo";
    static String BUSINESS_CDC_SOURCE_TABLE_NAME = "business_cdc";
    static String BUSINESS_DEST_TABLE_NAME = "business";

    // @BeforeAll
    public static void setUp() throws Exception {
    }

    // @AfterAll
    public static void tearDown() throws Exception {
        if(spark != null) {
            spark.stop();
        }
    }

    // @Test
    public void testIncrementalMergeSourceToDest() {
        // We could use the Java API to write data files, but we have a spark cluster for incremental reads and its cleaner
        // to do with spark-sql. https://tabular.io/blog/java-api-part-3/

        /*
        table business

        State 0: rows exist with ids [0,1]
        State 1: rows exist with ids [0,4]
        State 2: id=1 is updated with values
        State 3: id=2 is deleted

        Assertions
        cdc table has correct data set per snapshot
        merge table has correct data for each merge
        merging sequentially returns correct data, merging all snapshots at once returns correct data
         */
        Namespace demo = Namespace.of(SCHEMA_NAME);

        TableIdentifier businessCdcIdentifier = TableIdentifier.of(demo, BUSINESS_CDC_SOURCE_TABLE_NAME);
        Table businessCdcTable = catalog.loadTable(businessCdcIdentifier);

        TableIdentifier businessIdentifier = TableIdentifier.of(demo, BUSINESS_DEST_TABLE_NAME);
        Table businessTable = catalog.loadTable(businessIdentifier);

        Snapshot businessCdcSnapshot = businessCdcTable.currentSnapshot();
        Snapshot businessSnapshot = businessTable.currentSnapshot();

        /*
        System.out.println("business_cdc Snapshot Summary:  " + businessCdcSnapshot.summary());
        System.out.println("business Snapshot Summary:  " + businessSnapshot.summary());
         */
    }

    @Test
    public void testHybridCDCViewReturnsCorrectData() {
        ;
    }
}
