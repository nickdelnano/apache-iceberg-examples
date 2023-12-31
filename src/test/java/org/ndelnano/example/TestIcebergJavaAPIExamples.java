package org.ndelnano.example;

import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.ndelnano.example.Util.getConfiguration;

/*
This class assumes that the following services are accessible with the configuration in org.ndelnano.example.Util.getConfiguration
REST Catalog
Minio
Spark
 */

public class TestIcebergJavaAPIExamples {

    static RESTCatalog catalog;
    static Configuration conf;
    static Map<String, String> properties;
    static String CATALOG_NAME = "iceberg";

    static SparkSession spark;

    static String BUSINESS_CDC_SOURCE_TABLE_NAME = "business_cdc";
    static String BUSINESS_DEST_TABLE_NAME = "business";

    @BeforeAll
    public static void setUp() throws Exception {
        // Set up catalog
        catalog = new RESTCatalog();
        conf = new Configuration();
        Map<String, String> properties = getConfiguration();
        catalog.setConf(conf);
        catalog.initialize(CATALOG_NAME, properties);

        // Create tables
        // Iceberg Spark config is set by spark-iceberg container in docker-compose
        SparkConf sparkConf = new SparkConf().setAppName("incremental-read-tests").setMaster("local[2]");
        sparkConf.set("spark.master", "spark://localhost:7077");
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkConf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.uri", "http://localhost:8181");
        sparkConf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        sparkConf.set("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/wh/");
        sparkConf.set("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000");
        sparkConf.set("spark.sql.defaultCatalog", "iceberg");

        // Create a Spark session
        spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        spark.sql(
        "CREATE TABLE demo.business_cdc (\n" +
                "id STRING,\n" +
                "name STRING,\n" +
                "address STRING,\n" +
                "cdc_operation STRING,\n" +
                "event_time TIMESTAMP)\n" +
                "USING iceberg\n" +
                "PARTITIONED BY (event_time)\n" +
                "LOCATION 's3://warehouse/demo/business_cdc'\n" +
                "TBLPROPERTIES (\n" +
                "'format' = 'iceberg/parquet',\n" +
                "'format-version' = '2',\n" +
                "'write.parquet.compression-codec' = 'zstd')"
        );
        spark.sql(
                "CREATE TABLE demo.business (\n" +
                        "id STRING,\n" +
                        "name STRING,\n" +
                        "address STRING,\n" +
                        "event_time TIMESTAMP)\n" +
                        "USING iceberg\n" +
                        "PARTITIONED BY (event_time)\n" +
                        "LOCATION 's3://warehouse/demo/business'\n" +
                        "TBLPROPERTIES (\n" +
                        "'format' = 'iceberg/parquet',\n" +
                        "'format-version' = '2',\n" +
                        "'write.parquet.compression-codec' = 'zstd')"
        );
    }

    @AfterAll
    public static void tearDown() throws Exception {
        // spark.stop();
    }

    @Test
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
        Namespace orders = Namespace.of("orders");
        TableIdentifier paymentsTableIdentifier = TableIdentifier.of(orders, "payments");
        Table payments = catalog.loadTable(paymentsTableIdentifier);

        TableIdentifier paymentsMergeTableIdentifier = TableIdentifier.of(orders, "payments_merge");
        Table payments_merge = catalog.loadTable(paymentsMergeTableIdentifier);

        Snapshot paymentsSnapshot = payments.currentSnapshot();
        Snapshot paymentsMergeSnapshot = payments_merge.currentSnapshot();

        System.out.println("payments Snapshot Summary:  " + paymentsSnapshot.summary());
        System.out.println("payments_merge Snapshot Summary:  " + paymentsMergeSnapshot.summary());
    }

    @Test
    public void testHybridCDCViewReturnsCorrectData() {
        ;
    }
}
