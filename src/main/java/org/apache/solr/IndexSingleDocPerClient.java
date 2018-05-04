package org.apache.solr;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

public class IndexSingleDocPerClient {

    static AtomicInteger grandTotal = new AtomicInteger();
    static DecimalFormat decFormat = new DecimalFormat("###,###");
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static final List<String> words = new ArrayList<>();

    final static List<String> urls = new ArrayList<>();

    static String collection = null;
    static boolean commitAfterAdd = true;
    static int batchSize = 0;
    int numThreads = 0;
    static int numDocs = 0;
    static final List<IndexingField> fields = new ArrayList<>();

    //Adding a bare-bones logging functionality to dump data to a file
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static void main(String[] args) {

        System.setProperty("javax.net.ssl.keyStore", "/home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "secret");
        System.setProperty("javax.net.ssl.trustStore", "//home/ec2-user/keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "secret");

        if (args.length == 0 || args.length > 1 || "-h".equals(args[0]) || "--help".equals(args[0])) fullUsage();

        //Setting up the logger here
        try {
            GenericFileLogger.init();
        }catch(IOException ioe) {
            System.out.println("Failed to enable logging to file, quitting");
            return;
        }

        try {
            long start = System.currentTimeMillis();
            IndexSingleDocPerClient ima = new IndexSingleDocPerClient();
            ima.parseArgs(args);
            ima.doIt();
            System.out.println("Entire run took " + decFormat.format(System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
            usage();
        }
    }

    enum ARG_TYPE {NONE, URLS, COLLECTION, WORDFILE, INDEXING_THREADS, MAX_DOCS, FIELDS, BATCH_SIZE, COMMIT_AFTER_ADD};

    void parseArgs(String[] args) throws IOException {
        Path confPath = Paths.get(args[0]);
        Map<String, String> gatherFields = new HashMap<>();

        try (BufferedReader brConf = new BufferedReader(new InputStreamReader(new FileInputStream(confPath.toFile()), Charset.forName("UTF-8")))) {
            String line;
            ARG_TYPE type = ARG_TYPE.NONE;

            while ((line = brConf.readLine()) != null) {
                int pos = line.indexOf("//");
                if (line.contains("https")) {
                    if (pos >= 0 && line.indexOf("https") == -1) line = line.substring(0, pos);
                }
                else {
                    if (pos >= 0 && line.indexOf("http") == -1) line = line.substring(0, pos);
                }
                line = line.trim();
                if (line.length() == 0) continue;
                if (line.startsWith("COLLECTION:")) {
                    type = ARG_TYPE.COLLECTION;
                    continue;
                }
                if (line.startsWith("URLS:")) {
                    type = ARG_TYPE.URLS;
                    continue;
                }

                if (line.startsWith("WORDFILE:")) {
                    type = ARG_TYPE.WORDFILE;
                    continue;
                }
                if (line.startsWith("INDEXING_THREADS:")) {
                    type = ARG_TYPE.INDEXING_THREADS;
                    continue;
                }
                if (line.startsWith("MAX_DOCS:")) {
                    type = ARG_TYPE.MAX_DOCS;
                    continue;
                }
                if (line.startsWith("FIELDS:")) {
                    type = ARG_TYPE.FIELDS;
                    continue;
                }
                if (line.startsWith("BATCH_SIZE:")) {
                    type = ARG_TYPE.BATCH_SIZE;
                    continue;
                }
                if (line.startsWith("COMMIT_AFTER_ADD:")) {
                    type = ARG_TYPE.COMMIT_AFTER_ADD;
                    continue;
                }

                // the line is guaranteed to have content and we aren't processing a line with the indicated tag so just
                // go ahead. Comments have also been removed.
                switch (type) {
                    case NONE:
                        break;
                    case URLS:
                        urls.add(line);
                        break;
                    case COLLECTION:
                        collection = line;
                        break;
                    case WORDFILE:
                        Path wordPath = Paths.get(line);
                        try (BufferedReader brWords = new BufferedReader(new InputStreamReader(new FileInputStream(wordPath.toFile()), Charset.forName("UTF-8")))) {
                            String word;
                            while ((word = brWords.readLine()) != null) {
                                if (word.trim().length() == 0) continue;
                                words.add(word.trim());
                            }
                        }
                        break;
                    case INDEXING_THREADS:
                        numThreads = Integer.parseInt(line);
                        break;
                    case BATCH_SIZE:
                        batchSize = Integer.parseInt(line);
                        break;
                    case COMMIT_AFTER_ADD:
                        commitAfterAdd = Boolean.parseBoolean(line);
                        break;
                    case MAX_DOCS:
                        numDocs = Integer.parseInt(line);
                        break;
                    case FIELDS:
                        String[] fieldParts = line.trim().split("\\s");
                        gatherFields.put(fieldParts[0], line);
                        break;
                    default:
                        log("In default when parsing configs, what's wrong? " + line);
                        usage();
                }
            }
        }
        // Do this down here in case fields are defined before you get the words file.
        for (String val : gatherFields.values()) {
            fields.add(new IndexingField(val));
        }

        if (words.size() == 0 || numThreads == 0 || numDocs == 0 || urls.size() == 0
                || collection == null || fields.size() == 0 || batchSize == 0) {
            log("At least one parameter is wrong, figure it out. ");
            log("words.size(): " + words.size());
            log("numThreads: " + numThreads);
            log("numDocs: " + numDocs);
            log("urls: " + urls.size());
            log("collection: " + collection);
            log("fields: " + fields.size());
            log("batchSize: " + batchSize);
            usage();
        }
    }


    static Random rand = new Random();

    void doIt() throws InterruptedException {
        Thread[] threads = new Thread[numThreads];

        for (int idx = 0; idx < threads.length; ++idx) {
            threads[idx] = new Thread(new IndexingThread());
            threads[idx].start();
        }
        Thread reporter = new Thread(new ReporterThread(LOGGER));
        reporter.start();
        for (int idx = 0; idx < threads.length; ++idx) {
            threads[idx].join();
        }

        reporter.join();
    }

    static void usage() {
        log("Encountered something unexpected");
        log("To get the full help text, try specifying --help or -h");
        System.exit(-1);
    }

    static void fullUsage() {
        log("Usage: 'java -jar IndexSingleDocPerClient.jar config_file");
        log("");
        log("We're trying to generate the exceptions we're seeing with a client with this program.");
        log("Multiple programs can be executed in parallel.");
        log("The program will continue cycling forever, ctrl-c to kill it.");
        log("");
        log("-h or --help will print this and exit.");
        log("");
        log("The pattern at the client is to index a doc at a time, so for this exercise you may need to");
        log("use many threads and/or clients to generate sufficient load.");
        log("");
        log("The configuration file MUST have the following entries with the headings exactly as specified");
        log("headings are in ALL CAPS. Headings are all that can exist on a line, anything after is ignored.");
        log("anything after double slashes (//) is ignored.");
        log("The order of the headings is irrelevant.");
        log("ALL HEADINGS MUST BE PRESENT, THERE ARE NO DEFAULT VALUES!");
        log("");
        log("URLS:// URLs to randomly send HTTP updates to, form is 'http://host:port'. I can figure out how to add collection and update <G>");
        log("url1");
        log("url2");
        log("");
        log("COLLECTION: // the collection to send updates to, there should only be one.");
        log("my_collection");
        log("");
        log("WORDFILE:// The words should be one per line. This is used to synthesize text. Should be an absolute path.");
        log("path_to_list_of_words");
        log("");
        log("BATCH_SIZE: // how many docs to add per batch? This client uses 1!!!!");
        log("1");
        log("COMMIT_AFTER_ADD:// if true, commit after every batch. This client does this!");
        log("true");
        log("");
        log("INDEXING_THREADS:// the number of threads to fire up simultaneously");
        log("10");
        log("");
        log("MAX_DOCS://  doc IDs will be selected from 0-MAX_DOCS so we can keep from exploding the index.");
        log("10000000");
        log("");
        log("FIELDS:// Fields to populate. Don't bother with things like tdate, just use date etc..\n" +
                "Form is: 'field type max_tokens nullable(true/false) [unique_count]'.\n" +
                "    field: the name of the field to add. 'id' is assumed, don't specify it\n\n" +
                "    type: what kind of value. Allowed values: 'int', 'long', 'string', 'date' 'text' 'boolean'\n" +
                "           don't bother with 'tdate' and the like.\n\n" +
                "    max_tokens: There are two intrepretations. \n" +
                "          For text-based field, it's the number of words randomly selected from WORDFILE: to put in the field.\n" +
                "          For all other types, it's the it's the maximum number of times addField() is called on the doc.\n" +
                "                          The actual number of times will be between 1 and the number specified.\n" +
                "                          unless nullable is set to 'true' \n" +
                "    nullable: (use 'true' or 'false') is whether the field may be omitted.\n\n" +
                "    unique_count:the number of values used to populate the field. For instance, specifying 10 for an int field\n" +
                "           would put only 10 different numbers in the field in the entire corpus. IOW the cardinality of the field\n\n" +
                "    If more than one line has the same 'field', the last one wins."
        );
        log("my_string_field_a string 1 no // a single-valued string field with any word form WORDFILE as it's value and must always have a value");
        log("my_string_field_b string 10 yes 100 // a mult-valued string field with between 0 and 10 values per doc and 10 unique values across all docs.");
        log("");
        log("And be reasonable about things. Don't specify a unique count greater than the number of words in your file or , say, an  int type with a monstrous unique count.");
        log("This program doesn't do a lot of error checking");
        System.exit(-1);
    }

    static synchronized void log(String msg) {
        LOGGER.info(msg);
        //System.out.println(msg);
    }
}

class ReporterThread implements Runnable {

    long start = System.currentTimeMillis();
    Logger logger;

    public ReporterThread(Logger logger) {
        this.logger = logger;
    }


    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10000);
                long intervalSecs = (System.currentTimeMillis() - start) / 1000;
                int grand = IndexSingleDocPerClient.grandTotal.get();
                /*
                System.out.println(String.format("Indexed %s docs so far in %s seconds, avergage docs/second: %s",
                        IndexSingleDocPerClient.decFormat.format(grand),
                        IndexSingleDocPerClient.decFormat.format(intervalSecs),
                        IndexSingleDocPerClient.decFormat.format(grand / intervalSecs)));*/

                String str = String.format("Indexed %s docs so far in %s seconds, average docs/second: %s",
                        IndexSingleDocPerClient.decFormat.format(grand),
                        IndexSingleDocPerClient.decFormat.format(intervalSecs),
                        IndexSingleDocPerClient.decFormat.format(grand / intervalSecs));
                logger.info(str);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class IndexingThread implements Runnable {

    final List<HttpSolrClient> clients = new ArrayList<>();

    IndexingThread() {
        for (String url : IndexSingleDocPerClient.urls) {
            clients.add(new HttpSolrClient.Builder().withBaseSolrUrl(url + "/solr/" + IndexSingleDocPerClient.collection).build());
        }
    }

    Random rand = new Random();

    @Override
    public void run() {
        List<SolrInputDocument> docList = new ArrayList(IndexSingleDocPerClient.batchSize + 1);
        try {
            while (true) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", rand.nextInt(IndexSingleDocPerClient.numDocs));
                for (IndexingField field : IndexSingleDocPerClient.fields) {
                    for (String val : field.getVals()) {
                        doc.addField(field.getFieldName(), val);
                    }
                }
                docList.add(doc);

                if (docList.size() >= IndexSingleDocPerClient.batchSize) {
                    IndexSingleDocPerClient.grandTotal.addAndGet(docList.size());
                    HttpSolrClient client = clients.get(rand.nextInt(clients.size()));
                    for (int retry = 0; retry < 3; retry++) {
                        try {
                            client.add(docList);
                            break;
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("Caught a stupid exception, retrying.");
                            Thread.sleep(3000);
                        }
                    }
                    docList.clear();
                    if (IndexSingleDocPerClient.commitAfterAdd) {
                        client.commit();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                for (HttpSolrClient client : clients) {
                    client.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class IndexingField {
    String name;
    String type;
    int maxTokens;
    boolean nullable;
    int uniqueCount = Integer.MAX_VALUE;
    List<String> uniqueValues = new ArrayList<>();

    IndexingField(String line) {
        String[] parts = line.split("\\s");
        if (parts.length < 4 || parts.length > 5) {
            IndexSingleDocPerClient.log("This field specification is invalid: " + line);
            IndexSingleDocPerClient.usage();
        }
        try {
            name = parts[0];
            type = parts[1];
            maxTokens = Integer.parseInt(parts[2]);
            nullable = Boolean.parseBoolean(parts[3]);
            if (parts.length == 5) {
                uniqueCount = Integer.parseInt(parts[4]);
            }
        } catch (Exception e) {
            IndexSingleDocPerClient.log("Exception parsing line: " + line);
            throw e;
        }
        if (uniqueCount == Integer.MAX_VALUE) {
            return;
        }
        // Short circuit some gotchas.
        switch (type) {
            case "int":
            case "long":
                if (uniqueCount > 100_000_000) {
                    IndexSingleDocPerClient.log("UniqueCount for an int or long exceeds 100,000,000, be reasonable: " + line);
                    IndexSingleDocPerClient.usage();
                }
                break;
            case "string":
            case "text":
                if (uniqueCount > IndexSingleDocPerClient.words.size() * 100 / 75) {
                    IndexSingleDocPerClient.log("UniqueCount for a string or text field exceeds 75% of your word list size, be reasonable: " + line);
                    IndexSingleDocPerClient.usage();
                }

                break;
            case "date":
                if (uniqueCount > 1_000_000) {
                    IndexSingleDocPerClient.log("UniqueCount for a date field exceeds 1 million, be reasonable: " + line);
                    IndexSingleDocPerClient.usage();
                }

                break;
            case "boolean":
                if (uniqueCount != 2) {
                    IndexSingleDocPerClient.log("Oh PUHLEEEEEEAAAAASE, specifying a unique count for a boolean value?, be reasonable: " + line);
                    IndexSingleDocPerClient.usage();
                }
                break;
            default:
                IndexSingleDocPerClient.log("Weird field type, probably a programming error: " + line);
                IndexSingleDocPerClient.usage();
                break;
        }

        if ("boolean".equals(type)) {
            uniqueValues.add("true");
            uniqueValues.add("false");
            uniqueCount = 2;
        } else {
            Set<String> gather = new HashSet<>();
            while (gather.size() < uniqueCount) {
                switch (type) {
                    case "int":
                        gather.add(Integer.toString(Math.abs(IndexSingleDocPerClient.rand.nextInt())));
                        break;
                    case "long":
                        gather.add(Long.toString(Math.abs(IndexSingleDocPerClient.rand.nextLong())));
                        break;
                    case "string":
                    case "text":
                        gather.add(IndexSingleDocPerClient.words.get(IndexSingleDocPerClient.rand.nextInt(IndexSingleDocPerClient.words.size())));
                        break;
                    case "date":
                        Date dt = new Date(System.currentTimeMillis() - IndexSingleDocPerClient.rand.nextInt(100_000_000));
                        gather.add(IndexSingleDocPerClient.sdf.format(dt));
                        break;
                    case "boolean":
                        IndexSingleDocPerClient.log("Should not be here in Boolean: " + line);
                        IndexSingleDocPerClient.usage();
                        break;
                    default:
                        IndexSingleDocPerClient.log("Weird field type, probably a programming error down here: " + line);
                        IndexSingleDocPerClient.usage();
                        break;
                }
            }
            uniqueValues.addAll(gather);
        }
    }

    String getFieldName() {
        return name;
    }

    List<String> getVals() {
        List<String> vals = new ArrayList<>();
        if (nullable && (IndexSingleDocPerClient.rand.nextInt(10) % 10) == 0) return vals;

        if (maxTokens == 1) {
            vals.add(getAValue());
            return vals;
        }
        int lim = IndexSingleDocPerClient.rand.nextInt(maxTokens) + 1;
        if (type.equals("text")) {
            // It's inefficient to have a bunch of lists, so let's just assemble some tokens.
            StringBuilder sb = new StringBuilder();
            for (int idx = 0; idx < lim; ++idx) {
                sb.append(getAValue()).append(" ");
            }
            vals.add(sb.toString());
            return vals;
        }

        for (int idx = 0; idx < lim; ++idx) {
            vals.add(getAValue());
        }
        return vals;
    }

    String getAValue() {
        if (uniqueCount != Integer.MAX_VALUE) {
            return uniqueValues.get(IndexSingleDocPerClient.rand.nextInt(uniqueValues.size()));
        }
        switch (type) {
            case "int":
                return Integer.toString(Math.abs(IndexSingleDocPerClient.rand.nextInt()));
            case "long":
                return Long.toString(Math.abs(IndexSingleDocPerClient.rand.nextLong()));
            case "string":
            case "text":
                return IndexSingleDocPerClient.words.get(IndexSingleDocPerClient.rand.nextInt(IndexSingleDocPerClient.words.size()));
            case "date":
                Date dt = new Date(System.currentTimeMillis() - IndexSingleDocPerClient.rand.nextInt(100_000_000));
                return IndexSingleDocPerClient.sdf.format(dt);
            case "boolean":
                return (IndexSingleDocPerClient.rand.nextBoolean()) ? "true" : "false";
            default:
                IndexSingleDocPerClient.log("Weird field type, probably a programming error down here in getting a value.");
                IndexSingleDocPerClient.usage();
                break;
        }
        IndexSingleDocPerClient.log("We shouldn't be here either");
        return ""; // Shouldn't get here either.
    }
}

//Bad bad practise but creating one more class here
class GenericFileLogger {

    static private FileHandler fileTxt;
    static private SimpleFormatter formatterTxt;


    static public void init() throws IOException {

        Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.setUseParentHandlers(false);

        // suppress the logging output to the console by eliminating the ConsoleHandler
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        if (handlers[0] instanceof ConsoleHandler) {
            rootLogger.removeHandler(handlers[0]);
        }

        logger.setLevel(Level.INFO);
        String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(Calendar.getInstance().getTime());
        fileTxt = new FileHandler("report" + "_" + timeStamp + ".log");

        // create a text formatter
        formatterTxt = new SimpleFormatter();
        fileTxt.setFormatter(formatterTxt);
        logger.addHandler(fileTxt);
    }

}