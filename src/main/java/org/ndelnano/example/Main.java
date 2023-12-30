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

        String destinationTableName = "orders.payments";
        Table destinationTable = null;
        try {
            destinationTable = Spark3Util.loadIcebergTable(spark, destinationTableName);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }

        TableScan scan = destinationTable.newScan();

        TableScan filteredScan = scan.filter(Expressions.equal("id", 5));


        /*
        List<Map.Entry<String, String>> sourceWatermarkProperties =
        destinationTable.properties().entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(WATERMARK_PROP_PREFIX))
            .toList();

         # Some snapshots of dest may not have WATERMARK_PROP_PREFIX
         # Can check value of `operation` for that https://iceberg.apache.org/spec/#snapshots
         */

        System.out.printf("Hello and welcome!");

        for (int i = 1; i <= 5; i++) {
            System.out.println("i = " + i);
        }
    }
}