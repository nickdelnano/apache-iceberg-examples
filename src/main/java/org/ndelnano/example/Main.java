package org.ndelnano.example;

import static java.lang.String.format;
import static org.ndelnano.example.Util.getConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
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

    public static final String WATERMARK_PROP_PREFIX = "last-snapshot-id";

    public static void main(String[] args) {

        catalog = new RESTCatalog();
        conf = new Configuration();
        Map<String, String> properties = getConfiguration();
        catalog.setConf(conf);
        catalog.initialize(CATALOG_NAME, properties);

        // Create tables
        SparkConf sparkConf = new SparkConf().setAppName("incremental-read-tests").setMaster("local-cluster[2, 1, 200]");
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkConf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.uri", "http://localhost:8181");
        sparkConf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        sparkConf.set("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/wh/");
        sparkConf.set("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000");
        sparkConf.set("spark.sql.defaultCatalog", "iceberg");
        sparkConf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0");
        sparkConf.set("spark.executor.memory", "100m");
        sparkConf.set("spark.driver.memory", "100m");

        // Create a Spark session
        spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();


        spark.sql("INSERT INTO demo.business_cdc (\n" +
                        "VALUES\n" +
                        "(0, 'business_0', '0 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE), \n" +
                        "(1, 'business_1', '1 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE) \n" +
                        ")"
        );

        /*
        String sourceTableName = "orders.payments";
        String destinationTableName = "orders.payments_merge";

        Table destinationTable = null;
        try {
            destinationTable = Spark3Util.loadIcebergTable(spark, destinationTableName);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }


        // long endSnapshotId = sourceTable.currentSnapshot().snapshotId();
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
}