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

public class BulkIndexer2 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static String[] Strings = new String[3];
    private static Random r = new Random();

    public static void main(String args[]) throws Exception {

        /*System.setProperty("javax.net.ssl.keyStore", "/home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "secret");
        System.setProperty("javax.net.ssl.trustStore", "//home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "secret");*/

        final String zkHost = "localhost:";
        final CloudSolrClient client1 = new CloudSolrClient.Builder().withZkHost(zkHost+"9981").build();
        final CloudSolrClient client2 = new CloudSolrClient.Builder().withZkHost(zkHost+"9983").build();
        //final CloudSolrClient client3 = new CloudSolrClient.Builder().withZkHost(zkHost+"9985").build();
        //final CloudSolrClient client4 = new CloudSolrClient.Builder().withZkHost(zkHost+"9989").build();*/

        //final CloudSolrClient client = new CloudSolrClient.Builder().withZkHost("localhost:9983").build();

        final String collection = "test_export";
        client1.setDefaultCollection(collection);
        client2.setDefaultCollection(collection);
        /*client3.setDefaultCollection(collection);
        client4.setDefaultCollection(collection);*/
        //client.setDefaultCollection(collection);

        for (int i = 0; i < 3; i++) {
            Strings[i] = createSentance1(7);
        }

        System.out.println("start :: " + System.currentTimeMillis());

        List<Thread> threads = new ArrayList<>(100);

        final UpdateRequest updateRequest = new UpdateRequest();

        for (int k = 0; k < 1; k++) {
            final int id_k = k;
            Thread t = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 500; j++) {
                    //while (true) {
                        List<SolrInputDocument> docs = new ArrayList<>();
                        for (int i = 0; i < 5000; i++) {
                            /*SolrInputDocument document = new SolrInputDocument();
                            document.addField("id", UUID.randomUUID().toString());
                            document.addField("member_id_i", new Random().nextInt(3) % 3);
                            for (int z=0; z<10; z++) {
                                document.addField("subtotal"+z+"_i", 1000 + new Random().nextInt(3) % 3);
                            }
                            document.addField("quantity_l", Math.abs(new Random().nextLong() % 3));
                            document.addField("order_no_t", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr1_s", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr2_s", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr3_s", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr4_s", Strings[new Random().nextInt(3) % 3]);
                            document.addField("ship_addr5_s", Strings[new Random().nextInt(3) % 3]);*/
                            SolrInputDocument document = new SolrInputDocument();
                            long id = i + (j * 5000);
                            document.addField("id", id);
                            for (int z = 1; z <= 4; z++) {
                                document.addField("field" + z + "_s", id % 1000);
                                document.addField("field" + z + "_i", id % 1000);
                                document.addField("field" + z + "_d", id % 1000);
                                document.addField("field" + z + "_f", id % 1000);
                                document.addField("field" + z + "_l", id % 1000);
                                document.addField("field" + z + "_b", ThreadLocalRandom.current().nextBoolean());
                            }
                            for (int z = 1; z <= 4; z++) {
                                document.addField("fieldX" + z + "_s", id);
                                document.addField("fieldX" + z + "_i", id);
                                document.addField("fieldX" + z + "_d", id);
                                document.addField("fieldX" + z + "_f", id);
                                document.addField("fieldX" + z + "_l", id);
                                document.addField("fieldX" + z + "_b", ThreadLocalRandom.current().nextBoolean());
                            }
                            docs.add(document);
                        }
                        UpdateRequest updateRequest = new UpdateRequest();
                        updateRequest.add(docs);
                        try {
                            System.out.println("size: " + docs.size());
                            client1.request(updateRequest, collection);
                            client2.request(updateRequest, collection);
                            /*client3.request(updateRequest, collection);
                            client4.request(updateRequest, collection);*/
                            //client.request(updateRequest, collection);
                            //updateRequest.commit(client, collection);
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
        /*updateRequest.commit(client3, collection);
        updateRequest.commit(client4, collection);*/
        //updateRequest.commit(client, collection);

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
