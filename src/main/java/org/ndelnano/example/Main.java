package org.ndelnano.example;

import static java.lang.String.format;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.Spark3Util;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

public class Main {

    public static final String WATERMARK_PROP_PREFIX = "last-snapshot-id";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("incr-read-example").getOrCreate();

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

                /*
                spark
                        .read()
                        .format("iceberg")
                        .option("start-snapshot-id", startSnapshotId)
                        .option("end-snapshot-id", endSnapshotId)
                        .table(sourceTableName);

                 */

        try {
            inputDF.writeTo(destinationTableName)
                    .option(format("snapshot-property.%s.%s", WATERMARK_PROP_PREFIX, sourceTableName), String.valueOf(endSnapshotId))
                    .append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }


    }
}