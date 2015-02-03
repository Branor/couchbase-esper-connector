package example;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;

import java.io.IOException;

/**
 * Created by David on 04/02/2015.
 */
public class Generate {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(new Data("hello", 12.12));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Cluster cluster = CouchbaseCluster.create("localhost");
        Bucket bucket = cluster.openBucket("default");
        bucket.upsert(RawJsonDocument.create("_test", json));
    }
}