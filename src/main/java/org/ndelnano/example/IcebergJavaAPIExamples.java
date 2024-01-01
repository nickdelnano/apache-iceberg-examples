package org.ndelnano.example;

import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

import static org.ndelnano.example.Util.getCatalogConfigurationHostNetwork;

public class IcebergJavaAPIExamples {

    public static void main(String[] args) {
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
