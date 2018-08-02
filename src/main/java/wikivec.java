import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

public class wikivec {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String args[]) throws IOException {
        long startTime = System.nanoTime();

        StreamContext context = new StreamContext();
        ConnectionKeepAliveStrategy keepAliveStrat = new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                // we only close connections based on idle time, not ttl expiration
                return -1;
            }
        };

        String expr = "search(default,q=\"*:*\",sort=\"id desc\",fl=\"id,body_s\", qt=\"/export\")";
        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", expr);
        paramsLoc.set("qt", "/stream");
        String url = "http://localhost:8983/solr/default/";
        TupleStream solrStream = new SolrStream(url, paramsLoc);

        solrStream.setStreamContext(context);
        int tupleCounter = 0;
        try {
            solrStream.open();
            while (true) {
                final Tuple tuple = solrStream.read();
                if (tuple.EOF)
                    break;
                if (tupleCounter++ % 10 == 0) {
                    System.out.println("text=" + tuple.fields.get("body_s"));
                }
            }
            System.out.println("tuple counter final count: " + tupleCounter);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            solrStream.close();
        }
        System.out.println("index took" + TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) + " seconds to build");
    }
}
