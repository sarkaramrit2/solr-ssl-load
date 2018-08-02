import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QuerySolr_3 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String args[]) throws IOException, SolrServerException, InterruptedException {

        CloudSolrClient client = new CloudSolrClient.Builder().withZkHost("localhost:9983").build();
        client.setDefaultCollection("historical_stocks_data");

        SolrParams params = new ModifiableSolrParams()
                .add("q", "*:*")
                .add("qt", "/export");

        QueryResponse response = client.query(params);
        System.out.println(response);

        System.exit(0);
    }
}
