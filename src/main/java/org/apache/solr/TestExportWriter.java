package org.apache.solr;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestExportWriter {
    public static void main(String args[]) {
        //Providing the Solr string calls CLUSTERSTATUS twice per batch. Investigate why?
        CloudSolrClient client = new CloudSolrClient.Builder().withZkHost("localhost:9983").build();
        client.connect();
        client.setDefaultCollection("test_export");
        indexDocs(client);
    }

    private static void indexDocs(CloudSolrClient client) {
        int batchSize = 10_000;
        int numIters = 25_000_000 / batchSize;
        List<SolrInputDocument> docs = new ArrayList<>(batchSize);
        Random r = new Random();
        int x = 0;
        while (x < numIters) {
            for (int j = 0; j < batchSize; j++) {
                SolrInputDocument document = new SolrInputDocument();
                int id = (x * batchSize + j);
                document.addField("id",  id);
                for (int i = 1; i <= 4; i++) {
                    document.addField("field" + i + "_s", id % 100000);
                    document.addField("field" + i + "_i", id % 100000);
                }
                for (int i = 1; i <= 4; i++) {
                    document.addField("field2" + i + "_s", id);
                    document.addField("field2" + i + "_i", id);
                }
                docs.add(document);
            }
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.add(docs);
            try {
                System.out.println(client.request(updateRequest));
                docs.clear();
                x++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}