package org.apache.solr;


import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class BulkIndexer {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static String[] Strings = new String[3];
    private static Random r = new Random();

    public static void main(String args[]) throws Exception {

        /*System.setProperty("javax.net.ssl.keyStore", "/home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "secret");
        System.setProperty("javax.net.ssl.trustStore", "//home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "secret");*/

        //final String zkHost = "54.202.31.6:2181";
        final String zkHost1 = "35.162.107.109:9981";
        final String zkHost2 = "35.162.107.109:9983";
        final String zkHost3 = "35.162.107.109:9985";
        final CloudSolrClient client1 = new CloudSolrClient.Builder().withZkHost(zkHost1).build();
        final CloudSolrClient client2 = new CloudSolrClient.Builder().withZkHost(zkHost2).build();
        final CloudSolrClient client3 = new CloudSolrClient.Builder().withZkHost(zkHost3).build();

        final String collection = "test";
        client1.setDefaultCollection(collection);
        client2.setDefaultCollection(collection);
        client3.setDefaultCollection(collection);

        for (int i = 0; i < 3; i++) {
            Strings[i] = createSentance1(7);
        }

        System.out.println("start :: " + System.currentTimeMillis());

        List<Thread> threads = new ArrayList<>(100);

        final UpdateRequest updateRequest = new UpdateRequest();

        for (int k = 0; k < 1; k++) {
            int index = ThreadLocalRandom.current().nextInt(5);
            Thread t = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 5000; j++) {
                        //while (true) {
                        List<SolrInputDocument> docs = new ArrayList<>();
                        for (int i = 0; i < 5000; i++) {
                            SolrInputDocument document = new SolrInputDocument();
                            document.addField("id", UUID.randomUUID().toString());
                            document.addField("member_id_i", new Random().nextInt(3) % 3);
                            document.addField("quantity_l", Math.abs(new Random().nextLong() % 3));
                            document.addField("order_no_f", ThreadLocalRandom.current().nextFloat());
                            document.addField("ship_addr1_s", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr2_s", Strings[new Random().nextInt(3) % 3]);
                            docs.add(document);
                        }
                        UpdateRequest updateRequest = new UpdateRequest();
                        updateRequest.add(docs);
                        try {
                            System.out.println("updateRequest: " + updateRequest);
                            client1.request(updateRequest, collection);
                            client2.request(updateRequest, collection);
                            client3.request(updateRequest, collection);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        docs.clear();
                    }
                }
            };

            threads.add(t);
            t.start();
        }
        for (Thread thread : threads) thread.join();
        updateRequest.commit(client1, collection);
        updateRequest.commit(client2, collection);
        updateRequest.commit(client3, collection);

        System.out.println("end :: " + System.currentTimeMillis());
        System.exit(0);
    }

    private static String createSentance(int numWords) {
        //Sentence with numWords and 3-7 letters in each word
        StringBuilder sb = new StringBuilder(numWords * 5);
        for (int i = 0; i < numWords; i++) {
            sb.append("abcd");
        }
        return sb.toString();
    }

    private static String createSentance1(int numWords) {
        //Sentence with numWords and 3-7 letters in each word
        StringBuilder sb = new StringBuilder(numWords * 2);
        for (int i = 0; i < numWords; i++) {
            sb.append(TestUtil.randomSimpleString(r, 1, 1));
        }
        return sb.toString();
    }


}
