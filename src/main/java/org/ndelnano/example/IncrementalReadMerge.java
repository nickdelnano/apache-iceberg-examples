package org.ndelnano.example;

import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static org.ndelnano.example.Util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.spark.CommitMetadata;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class IncrementalReadMerge {

    SparkSession spark;
    RESTCatalog catalog;
    Configuration conf;
    Namespace demoIcebergNamespace;
    static final String CATALOG_NAME = "iceberg";
    static final String SCHEMA_NAME = "demo";
    static final String BUSINESS_CDC_SOURCE_TABLE_NAME = "business_cdc";
    static final String BUSINESS_DEST_TABLE_NAME = "business";

    static final String LAST_SNAPSHOT_ID_WATERMARK = "last-snapshot-id";

    public IncrementalReadMerge() {
        this.catalog = new RESTCatalog();
        this.conf = new Configuration();
        Map<String, String> properties = getCatalogConfigurationDockerNetwork();
        this.catalog.setConf(conf);
        this.catalog.initialize(CATALOG_NAME, properties);
        this.demoIcebergNamespace = Namespace.of(SCHEMA_NAME);

        // Create a Spark session
        SparkConf sparkConf = new SparkConf().setAppName("incremental-read-tests");
        this.spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Create tables business_cdc and business. Recreate them if already exists.
        setupSparkTables();
    }

    public static void main(String[] args) {
        IncrementalReadMerge incrementalReadMerge = new IncrementalReadMerge();
        // Seed data into business_cdc table
        incrementalReadMerge.seedData();

        incrementalReadMerge.incrementalMerge(BUSINESS_CDC_SOURCE_TABLE_NAME, BUSINESS_DEST_TABLE_NAME);

        incrementalReadMerge.seedData();
        incrementalReadMerge.incrementalMerge(BUSINESS_CDC_SOURCE_TABLE_NAME, BUSINESS_DEST_TABLE_NAME);

        incrementalReadMerge.spark.sql(String.format("SELECT * FROM %s.%s", SCHEMA_NAME, BUSINESS_DEST_TABLE_NAME)).show();
    }

    public void incrementalMerge(String sourceTableName, String destTableName) {
        Table sourceTable = getIcebergTable(sourceTableName);
        Table destTable = getIcebergTable(destTableName);

        Long sourceLastCommittedSnapshot = sourceTable.currentSnapshot().snapshotId();
        // Get sourceTable commit watermark from destTable - the last commit merged to it
        Long destLastCommittedSnapshot = IncrementalReadMerge.getCommitWatermark(destTable, sourceTableName);

        // If destTable has not received any commits yet, do not specify start-snapshot-id and read from the beginning of the table
        // start-snapshot-id is exclusive, end-snapshot-id is inclusive
        if (destLastCommittedSnapshot == null) {
            spark.sql(String.format("CALL iceberg.system.create_changelog_view(\n" +
                            "table => 'iceberg.%s.%s',\n" +
                            "options => map('end-snapshot-id', '%s'))"
                    , SCHEMA_NAME, sourceTableName, sourceLastCommittedSnapshot));
        } else {
            spark.sql(String.format("CALL iceberg.system.create_changelog_view(\n" +
                            "table => 'iceberg.%s.%s',\n" +
                            "options => map('start-snapshot-id', '%s', 'end-snapshot-id', '%s'))"
                    , SCHEMA_NAME, sourceTableName, destLastCommittedSnapshot, sourceLastCommittedSnapshot));
        }

        /*
        "CommitMetadata provides an interface to add custom metadata to a snapshot summary during a SQL execution,
        which can be beneficial for purposes such as auditing or change tracking."
        https://iceberg.apache.org/docs/latest/spark-configuration/#write-options
         */
        CommitMetadata.withCommitProperties(
                Map.of(format("%s.%s", LAST_SNAPSHOT_ID_WATERMARK, sourceTableName), String.valueOf(sourceLastCommittedSnapshot)),
                () -> {
                    spark.sql(String.format(
                            "INSERT INTO %s.%s (select id, name, address, event_time from %s" + "_changes)",
                    SCHEMA_NAME,destTableName, sourceTableName)).show();
                    return 0;
                },
                RuntimeException.class);
    }

    public static Long getCommitWatermark(Table table, String sourceTableName) {
        Snapshot snapshot = table.currentSnapshot();

        // snapshot will be null if the table hasn't committed any writes
        if (snapshot == null) {
            return null;
        } else {
            return parseLong(snapshot.summary().get(String.format("%s.%s", LAST_SNAPSHOT_ID_WATERMARK, sourceTableName)));
        }
    }

    /*
    Return org.apache.iceberg.Table object for tableName. Assume that all tables exist in the same namespace.
     */
    public Table getIcebergTable(String tableName) {
        TableIdentifier tableNameIdentifier = TableIdentifier.of(demoIcebergNamespace, tableName);
        return catalog.loadTable(tableNameIdentifier);
    }

    public void setupSparkTables() {
        // Drop tables if exists
        spark.sql(String.format(
                "DROP TABLE IF EXISTS %s.%s"
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)
        ).show();

        spark.sql(String.format(
                "DROP TABLE IF EXISTS %s.%s"
                , SCHEMA_NAME, BUSINESS_DEST_TABLE_NAME)
        ).show();

        // Create tables
        spark.sql(String.format(
                "CREATE SCHEMA IF NOT EXISTS %s"
                , SCHEMA_NAME)
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
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)
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
                        , SCHEMA_NAME, BUSINESS_DEST_TABLE_NAME)
        ).show();
    }

    public void seedData() {
        // Commit 1 -- INSERT id=(0,1)
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(0, 'business_0', '0 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE), \n" +
                        "(1, 'business_1', '1 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 30 MINUTE) \n" +
                        ")"
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 2 -- INSERT id=(2,3)
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(2, 'business_2', '2 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 29 MINUTE), \n" +
                        "(3, 'business_3', '3 Main St', 'INSERT',  CURRENT_TIMESTAMP() - INTERVAL 29 MINUTE) \n" +
                        ")"
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 3 -- UPDATE id=1
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(1, 'business_1', '1111 Main St', 'UPDATE',  CURRENT_TIMESTAMP() - INTERVAL 28 MINUTE) \n" +
                        ")"
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

        // Commit 4 -- DELETE id=2
        spark.sql(String.format("INSERT INTO %s.%s (\n" +
                        "VALUES\n" +
                        "(2, 'business_2', '2 Main St', 'DELETE',  CURRENT_TIMESTAMP() - INTERVAL 28 MINUTE) \n" +
                        ")"
                , SCHEMA_NAME, BUSINESS_CDC_SOURCE_TABLE_NAME)).show();

    }
 /*
        // Snapshot operation values https://iceberg.apache.org/spec/#snapshots, https://iceberg.apache.org/javadoc/1.1.0/org/apache/iceberg/DataOperations.html
        // TODO check how snapshot rollbacks are handled
        switch (op) {
            case DataOperations.APPEND:
                ;
            case DataOperations.REPLACE:
                ;
            case DataOperations.DELETE:
            case DataOperations.OVERWRITE:
                throw new IllegalStateException(
                        String.format(
                                "Source CDC table should not have snapshot operation: %s (snapshot id %s)",
                                op.toLowerCase(Locale.ROOT), snapshot.snapshotId()));
            default:
                throw new IllegalStateException(
                        String.format(
                                "Cannot process unknown snapshot operation: %s (snapshot id %s)",
                                op.toLowerCase(Locale.ROOT), snapshot.snapshotId()));
        }

         */
}