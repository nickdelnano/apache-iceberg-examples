package org.ndelnano.example;

import static java.lang.String.format;
import static org.ndelnano.example.Util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.spark.Spark3Util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.util.Map;

public class Main {

    static RESTCatalog catalog;
    static Configuration conf;
    static SparkSession spark;
    static String CATALOG_NAME = "iceberg";
    static String SCHEMA_NAME = "demo";
    static String BUSINESS_CDC_SOURCE_TABLE_NAME = "business_cdc";
    static String BUSINESS_DEST_TABLE_NAME = "business";

    static Namespace demoIcebergNamespace;

    public static final String WATERMARK_PROP_PREFIX = "last-snapshot-id";

    public static void main(String[] args) {

        catalog = new RESTCatalog();
        conf = new Configuration();
        Map<String, String> properties = getCatalogConfigurationDockerNetwork();
        catalog.setConf(conf);
        catalog.initialize(CATALOG_NAME, properties);

        demoIcebergNamespace = Namespace.of(SCHEMA_NAME);

        // Create tables
        SparkConf sparkConf = new SparkConf().setAppName("incremental-read-tests");

        // Create a Spark session
        spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

         spark.sql("show catalogs;").show();
        // Drop tables if exists
        spark.sql(String.format(
                "DROP TABLE IF EXISTS %s.%s"
                ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)
        ).show();

        spark.sql(String.format(
                "DROP TABLE IF EXISTS %s.%s"
                ,SCHEMA_NAME, BUSINESS_DEST_TABLE_NAME)
        ).show();

        // Create tables
        spark.sql(String.format(
                "CREATE SCHEMA IF NOT EXISTS %s"
                ,SCHEMA_NAME)
        ).show();
        spark.sql(String.format(
        "CREATE TABLE %s.%s (\n" +
                "id STRING,\n" +
                "name STRING,\n" +
                "address STRING,\n" +
                "cdc_operation STRING,\n" +
                "event_time TIMESTAMP)\n" +
                "USING iceberg\n" +
                "PARTITIONED BY (id)\n" +
                "LOCATION 's3://warehouse/demo/business_cdc'\n" +
                "TBLPROPERTIES (\n" +
                "'format' = 'iceberg/parquet',\n" +
                "'format-version' = '2',\n" +
                "'write.parquet.compression-codec' = 'zstd')"
            ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)
        ).show();
        spark.sql(
                String.format("CREATE TABLE %s.%s (\n" +
                        "id STRING,\n" +
                        "name STRING,\n" +
                        "address STRING,\n" +
                        "event_time TIMESTAMP)\n" +
                        "USING iceberg\n" +
                        "PARTITIONED BY (id)\n" +
                        "LOCATION 's3://warehouse/demo/business'\n" +
                        "TBLPROPERTIES (\n" +
                        "'format' = 'iceberg/parquet',\n" +
                        "'format-version' = '2',\n" +
                        "'write.parquet.compression-codec' = 'zstd')"
                ,SCHEMA_NAME, BUSINESS_DEST_TABLE_NAME)
        ).show();

        // Seed data into business_cdc table
        // Commit 1-- INSERT id=(0,1)
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(0, 'business_0', '0 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE), \n" +
                        "(1, 'business_1', '1 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE) \n" +
                        ")"
        ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 2 -- INSERT id=(2, 3)
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(2, 'business_2', '2 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 29 MINUTE), \n" +
                        "(3, 'business_3', '3 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 29 MINUTE) \n" +
                        ")"
                ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 3 -- UPDATE id=1
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(1, 'business_1', '1111 Main St', 'UPDATE',  CURRENT_TIMESTAMP() - INTERVAL 28 MINUTE) \n" +
                        ")"
                ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 4 -- DELETE id=2
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(2, 'business_2', '2 Main St', 'DELETE',  CURRENT_TIMESTAMP() - INTERVAL 28 MINUTE) \n" +
                        ")"
                ,SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        spark.sql(String.format("SELECT * FROM %s.%s",SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        int number_of_commits = 1;
        incrementalMerge(BUSINESS_CDC_SOURCE_TABLE_NAME, BUSINESS_DEST_TABLE_NAME, number_of_commits);

        /*
        long endSnapshotId = 6234076645174348707L;

        Dataset<Row> inputDF = spark.sql("select * from iceberg.orders.payments VERSION AS OF " + endSnapshotId);

                spark
                        .read()
                        .format("iceberg")
                        .option("start-snapshot-id", startSnapshotId)
                        .option("end-snapshot-id", endSnapshotId)
                        .table(sourceTableName);


        try {
            inputDF.writeTo(destinationTableName)
                    .option(format("snapshot-property.%s.%s", WATERMARK_PROP_PREFIX, sourceTableName), String.valueOf(endSnapshotId))
                    .append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
        */
    }

    public static void incrementalMerge(String source_table, String dest_table, int number_of_commits) {
        long endSnapshotId = sourceTable.currentSnapshot().snapshotId();
    }

    public static Table getIcebergTableForTableName(String table_name) {
        TableIdentifier paymentsTableIdentifier = TableIdentifier.of(orders, "payments");
        Table payments = catalog.loadTable(paymentsTableIdentifier);

        TableIdentifier paymentsMergeTableIdentifier = TableIdentifier.of(orders, "payments_merge");
        Table payments_merge = catalog.loadTable(paymentsMergeTableIdentifier);

        Snapshot paymentsSnapshot = payments.currentSnapshot();
        Snapshot paymentsMergeSnapshot = payments_merge.currentSnapshot();
    }
}