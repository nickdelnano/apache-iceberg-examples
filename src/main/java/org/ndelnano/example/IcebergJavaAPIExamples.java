package org.ndelnano.example;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;

import org.apache.iceberg.aws.AwsProperties;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.ndelnano.example.Util.getConfiguration;

public class IcebergJavaAPIExamples {

    public static void main(String[] args) {

        Map<String, String> properties = getConfiguration();

        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        Namespace orders = Namespace.of("orders");
        TableIdentifier paymentsTableIdentifier = TableIdentifier.of(orders, "payments");
        Table payments = catalog.loadTable(paymentsTableIdentifier);

        TableIdentifier paymentsMergeTableIdentifier = TableIdentifier.of(orders, "payments_merge");
        Table payments_merge = catalog.loadTable(paymentsMergeTableIdentifier);

        Snapshot paymentsSnapshot = payments.currentSnapshot();
        Snapshot paymentsMergeSnapshot = payments_merge.currentSnapshot();

        System.out.print("payments Snapshot Summary:  " + paymentsSnapshot.summary());
        System.out.print("payments_merge Snapshot Summary:  " + paymentsMergeSnapshot.summary());

        String op = paymentsSnapshot.operation();


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
}
