package org.ndelnano.example;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.HashMap;
import java.util.Map;

public class Util {

    public static Map<String, String> getCatalogConfigurationDockerNetwork() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "http://rest:8181");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/wh/");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, "http://minio:9000");

        return properties;
    }

    public static Map<String, String> getCatalogConfigurationHostNetwork() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "http://localhost:8181");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/wh/");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, "http://localhost:9000");

        return properties;
    }
}
