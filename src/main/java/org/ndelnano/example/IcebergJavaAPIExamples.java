package org.ndelnano.example;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;

import org.apache.iceberg.aws.AwsProperties;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;

public class IcebergJavaAPIExamples {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "http://localhost:8181");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/wh/");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, "http://localhost:9000");

        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);
        
        Namespace orders = Namespace.of("orders");
        TableIdentifier name = TableIdentifier.of(orders, "payments");
        Table payments = catalog.loadTable(name);

        System.out.println(payments.toString());
    }
}
