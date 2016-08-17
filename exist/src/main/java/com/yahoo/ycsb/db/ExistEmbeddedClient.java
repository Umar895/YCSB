package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.apache.commons.io.Charsets;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.ElementImpl;
import org.exist.security.AuthenticationException;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.security.xacml.AccessContext;
import org.exist.source.Source;
import org.exist.source.StringSource;
import org.exist.storage.DBBroker;
import org.exist.storage.XQueryPool;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.Txn;
import org.exist.util.Configuration;
import org.exist.storage.BrokerPool;
import org.exist.util.DatabaseConfigurationException;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.CompiledXQuery;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * eXist binding for <a href="http://exist-db.org/">eXist</a>.
 *
 * See {@code exist/README.md} for details.
 */
public class ExistEmbeddedClient extends DB {

  //TODO(AR) need to check appropriate locking of collections and documents
  //TODO(AR) may be quicker to work directly with eXist's DOM/Collections

  private static final String EXIST_DIR = "exist.dir";
  private static final String EXIST_CONFIG = "exist.conf";
  private static final String EXIST_MULTIPLE_DOCS = "exist.multiple-docs";

  private static Path existDir = null;
  private static BrokerPool brokerPool = null;
  private static Subject admin = null;
  private static boolean multipleDocs = false;

  private static final XmldbURI TEST_DATA_COLLECTION = XmldbURI.DB.append("ycsb");
  private static final XmldbURI TEST_CONFIG_COLLECTION = XmldbURI.CONFIG_COLLECTION_URI.append(TEST_DATA_COLLECTION);
  private static final XmldbURI SINGLE_DOCUMENT_NAME = XmldbURI.create("ycsb-single-doc.xml");
  private static final AtomicInteger REFERENCES = new AtomicInteger();

  private static final String COLLECTION_CONF = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
      "<collection xmlns=\"http://exist-db.org/collection-config/1.0\">\n"+
      "    <index>\n"+
      "        <range>\n"+
      "            <create qname=\"@key\" type=\"xs:string\"/>\n"+
      "            <create qname=\"@value\" type=\"xs:string\"/>\n"+
      "        </range>\n"+
      "    </index>\n"+
      "</collection>";

  @Override
  public void init() throws DBException {
    super.init();
    if(brokerPool == null) {
      synchronized (ExistEmbeddedClient.class) {
        if(brokerPool == null) {
          try {
            this.existDir = Paths.get(getProperties().getProperty(EXIST_DIR));
            if(!Files.exists(existDir)) {
              Files.createDirectories(existDir);
            }

            final String conf = getProperties().getProperty(EXIST_CONFIG, "conf.xml");
            System.out.println("conf.xml: " + conf);

            final Configuration config = new Configuration(conf);
            config.setProperty("db-connection.data-dir", existDir.toAbsolutePath().toString());
            config.setProperty("db-connection.recovery.journal-dir", existDir.toAbsolutePath().toString());
            BrokerPool.configure(1, 5, config);
            this.brokerPool = BrokerPool.getInstance();

            this.multipleDocs = Boolean.parseBoolean(getProperties().getProperty(EXIST_MULTIPLE_DOCS, "false"));

            this.admin = brokerPool.getSecurityManager().authenticate("admin", "");

            try(final DBBroker broker = brokerPool.get(admin);
                final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

              //store index config if not already present
              if (broker.getCollection(TEST_CONFIG_COLLECTION) == null) {
                final Collection configCollection = broker.getOrCreateCollection(transaction, TEST_CONFIG_COLLECTION);
                broker.saveCollection(transaction, configCollection);
                final XmldbURI configDocName = XmldbURI.create("collection.xconf");
                final IndexInfo indexInfo = configCollection.validateXMLResource(
                    transaction, broker, configDocName, COLLECTION_CONF);
                configCollection.store(transaction, broker, indexInfo, COLLECTION_CONF, false);
              }

              //prepare the data collection if it doesn't yet exist
              if(broker.getCollection(TEST_DATA_COLLECTION) == null) {
                final Collection testCollection = broker.getOrCreateCollection(transaction, TEST_DATA_COLLECTION);
                broker.saveCollection(transaction, testCollection);
              }

              transaction.commit();
            }
          } catch(final IOException | EXistException | DatabaseConfigurationException | AuthenticationException
            | PermissionDeniedException | SAXException | LockException e) {
            throw new DBException(e);
          }
        }
      }
    }
    REFERENCES.incrementAndGet();
  }

  @Override
  public void cleanup() throws DBException {
    try {
      super.cleanup();
      if (REFERENCES.get() == 1) {
        this.brokerPool.shutdown();
      }
    } finally {
      REFERENCES.decrementAndGet();
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final HashMap<String, ByteIterator> result) {

    try(final DBBroker broker = brokerPool.get(admin);
        final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

      final XmldbURI collectionUri = TEST_DATA_COLLECTION.append(table);
      final Collection collection = broker.getOrCreateCollection(transaction, collectionUri);

      final XmldbURI docName;
      if(multipleDocs) {
        //one document per key
        docName = XmldbURI.create(key + ".xml");
      } else {
        //one document per table
        docName = XmldbURI.create(table + ".xml");
      }

      if(!collection.hasDocument(broker, docName)) {
        transaction.commit();
        return Status.NOT_FOUND;
      } else {
        final String docUri = TEST_DATA_COLLECTION.append(table).append(docName).toString();
        final StringBuilder query = new StringBuilder();
        query.append("doc('").append(docUri).append("')");

        if(multipleDocs) {
          //one document per key
          query.append("//value");
        } else {
          //one document per table
          query.append(")//values[@key eq '").append(key).append("']/value");
        }

        if(fields != null) {
          fieldsToPredicate(query, fields);
        }

        final Sequence results = executeQuery(broker, new StringSource(query.toString()));
        if(results == null || results.isEmpty()) {
          transaction.commit();
          return Status.NOT_FOUND;
        } else {
          final SequenceIterator it = results.iterate();
          while(it.hasNext()) {
            final Item item = it.nextItem();
            if(item instanceof ElementImpl) {
              final ElementImpl e = ((ElementImpl)item);
              result.put(
                  e.getAttribute("key"),
                  new ByteArrayByteIterator(e.getAttribute("value").getBytes(StandardCharsets.UTF_8))
              );
            } else {
              System.err.println("Expected ElementImpl, but found: " + item.getClass());
              transaction.abort();
              return Status.ERROR;
            }
          }
        }
      }

      transaction.commit();

      return Status.OK;
    } catch(final EXistException | PermissionDeniedException | IOException | SAXException | XPathException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {

    final StringBuilder query = new StringBuilder();

    try(final DBBroker broker = brokerPool.get(admin);
        final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

      final XmldbURI collectionUri = TEST_DATA_COLLECTION.append(table);
      final Collection collection = broker.getOrCreateCollection(transaction, collectionUri);

      if(multipleDocs) {
        //one document per key

        final XmldbURI desiredDocument = XmldbURI.create(startkey + ".xml");

        //seek to the desired document in the collection
        final Iterator<DocumentImpl> docs = collection.iterator(broker);
        DocumentImpl doc = null;
        while(docs.hasNext()) {
          doc = docs.next();
          if(doc.getFileURI().equals(desiredDocument)) {
            break;
          }
        }

        if(doc == null) {
          System.err.println("Could not seek to document: " + desiredDocument);
          transaction.abort();
          return Status.ERROR;
        }

        final List<XmldbURI> desiredDocs = new ArrayList<>();
        desiredDocs.add(doc.getURI());

        for(int i = 0; i < recordcount - 1 && docs.hasNext(); i++) {
          doc = docs.next();
          desiredDocs.add(doc.getURI());
        }

        if(desiredDocs.size() != recordcount) {
          System.err.println("Could not locate enough records");
          transaction.abort();
          return Status.ERROR;
        }

        //perform a query on the desiredDocs, by using (doc(1), doc(2) .. doc(n))
        query.append("let $docs := (");
        for(int i = 0; i < desiredDocs.size(); i++) {
          query.append("doc('").append(desiredDocs.get(i).toString()).append("')");
          if(i + 1 < desiredDocs.size()) {
            query.append(",");
          }
        }
        query.append(") return for $doc in $docs return <values>{$doc/values/value");
        if(fields != null) {
          fieldsToPredicate(query, fields);
        }
        query.append("}</values>");

      } else {
        //one document per table
        final XmldbURI docName = XmldbURI.create(table + ".xml");
        final XmldbURI docUri = collectionUri.append(docName);

        query.append("let $doc := doc('")
          .append(docUri)
          .append("'), $start-element := $doc//values[@key eq '")
          .append(startkey)
          .append("'] return (<values>{$start-element/value");
        if(fields != null) {
          fieldsToPredicate(query, fields);
        }
        query.append(
          "}</values>, for $vs in $start-element/following-sibling::values[position() lt $start-element/position() + "
        ).append(recordcount - 1).append("] return <values>{$vs/value");
        if(fields != null) {
          fieldsToPredicate(query, fields);
        }
        query.append("}</values>)");
      }

      final Sequence results = executeQuery(broker, new StringSource(query.toString()));
      if(results == null || results.isEmpty()) {
        transaction.commit();
        return Status.NOT_FOUND;
      } else {
        final SequenceIterator it = results.iterate();
        while(it.hasNext()) {
          final Item item = it.nextItem();
          if(item instanceof ElementImpl) {
            final ElementImpl e = ((ElementImpl)item);
            final HashMap<String, ByteIterator> fieldValues = new HashMap<>();
            final NodeList vs = e.getElementsByTagName("value");
            for(int i = 0; i < vs.getLength(); i++) {
              final ElementImpl value = (ElementImpl)vs.item(i);
              fieldValues.put(
                  value.getAttribute("key"),
                  new ByteArrayByteIterator(value.getAttribute("value").getBytes(StandardCharsets.UTF_8))
              );
            }
            result.add(fieldValues);
          } else {
            System.err.println("Expected ElementImpl, but found: " + item.getClass());
            transaction.abort();
            return Status.ERROR;
          }
        }
      }

      transaction.commit();

      return Status.OK;
    } catch(final EXistException | PermissionDeniedException | IOException | SAXException | XPathException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final HashMap<String, ByteIterator> values) {
    try(final DBBroker broker = brokerPool.get(admin);
        final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

      final XmldbURI collectionUri = TEST_DATA_COLLECTION.append(table);
      final Collection collection = broker.getOrCreateCollection(transaction, collectionUri);

      final XmldbURI docName;
      if(multipleDocs) {
        //one document per key
        docName = XmldbURI.create(key + ".xml");
      } else {
        //one document per table
        docName = XmldbURI.create(table + ".xml");
      }

      if(!collection.hasDocument(broker, docName)) {
        transaction.commit();
        return Status.NOT_FOUND;
      } else {
        final XmldbURI docUri = collectionUri.append(docName);
        final StringBuilder query = new StringBuilder();
        for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
          query.append("update value doc('").append(docUri).append("')//value[@key eq '").append(value.getKey())
            .append("'] with '").append(new String(value.getValue().toArray(), StandardCharsets.UTF_8))
            .append("'").append(System.getProperty("line.separator"));
        }

        executeQuery(broker, new StringSource(query.toString()));

        transaction.commit();

        return Status.OK;
      }
    } catch(final EXistException | PermissionDeniedException | IOException | SAXException | XPathException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final HashMap<String, ByteIterator> values) {
    try(final DBBroker broker = brokerPool.get(admin);
        final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

      final XmldbURI collectionUri = TEST_DATA_COLLECTION.append(table);
      final Collection collection = broker.getOrCreateCollection(transaction, collectionUri);

      if (multipleDocs) {

        //one document per key

        //insert a new doc for the key
        final XmldbURI docName = XmldbURI.create(key + ".xml");
        final String xml = asXmlDoc(values);
        collection.getLock().acquire(Lock.WRITE_LOCK);
        try {
          final IndexInfo indexInfo = collection.validateXMLResource(transaction, broker, docName, xml);
          collection.store(transaction, broker, indexInfo, xml, false);
        } finally {
          collection.getLock().release(Lock.WRITE_LOCK);
        }
      } else {

        //one document per table

        final XmldbURI docName = XmldbURI.create(table + ".xml");
        if(!collection.hasDocument(broker, SINGLE_DOCUMENT_NAME)) {
          //insert the first version of the document for the table
          final String xml = asXmlDoc(key, values);
          collection.getLock().acquire(Lock.WRITE_LOCK);
          try {
            final IndexInfo indexInfo = collection.validateXMLResource(transaction, broker, docName, xml);
            collection.store(transaction, broker, indexInfo, xml, false);
          } finally {
            collection.getLock().release(Lock.WRITE_LOCK);
          }
        } else {
          //update the document
          final String docPath = collectionUri.append(docName).toString();
          executeQuery(
              broker,
              new StringSource("update insert " + asXmlElement(key, values) + " into doc('" + docPath + "')/keys")
          );
        }
      }

      transaction.commit();

      return Status.OK;

    } catch(final EXistException | PermissionDeniedException | IOException | SAXException | LockException
      | XPathException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {

    try(final DBBroker broker = brokerPool.get(admin);
        final Txn transaction = brokerPool.getTransactionManager().beginTransaction()) {

      final XmldbURI collectionUri = TEST_DATA_COLLECTION.append(table);
      final Collection collection = broker.getOrCreateCollection(transaction, collectionUri);

      if(multipleDocs) {

        //one document per key
        final XmldbURI docName = XmldbURI.create(key + ".xml");
        try {
          collection.getLock().acquire(Lock.WRITE_LOCK);
          try {
            final DocumentImpl doc = collection.getDocument(broker, docName);
            if (doc == null) {
              transaction.commit();
              return Status.NOT_FOUND;
            } else {
              broker.removeXMLResource(transaction, doc);
            }
          } finally {
            collection.getLock().release(Lock.WRITE_LOCK);
          }
        } catch(final LockException e) {
          e.printStackTrace();
          transaction.abort();
          return Status.ERROR;
        }
      } else {

        //one document per table

        //remove from the document

        final XmldbURI docName = XmldbURI.create(table + ".xml");
        if(!collection.hasDocument(broker, SINGLE_DOCUMENT_NAME)) {
          transaction.abort();
          return Status.NOT_FOUND;
        } else {
          final String docPath = collectionUri.append(docName).toString();
          executeQuery(broker, new StringSource("update delete doc('" + docPath + "')//values[@key eq '" + key + "']"));
        }
      }

      transaction.commit();

      return Status.OK;

    } catch(final EXistException | PermissionDeniedException | IOException | SAXException | XPathException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void fieldsToPredicate(final StringBuilder builder, final java.util.Collection<String> fields) {
    builder.append("[@key = (");
    final String[] arry = fields.toArray(new String[fields.size()]);
    for(int i = 0; i < arry.length; i++) {
      builder.append("\"");
      builder.append(arry[i]);
      builder.append("\"");
      if(i + 1 < arry.length) {
        builder.append(",");
      }
    }
    builder.append(")]");
  }

  private Sequence executeQuery(final DBBroker broker, final Source source)
      throws PermissionDeniedException, IOException, XPathException {
    final XQuery xqueryService = broker.getBrokerPool().getXQueryService();
    final XQueryPool xqueryPool = broker.getBrokerPool().getXQueryPool();

    CompiledXQuery compiled = xqueryPool.borrowCompiledXQuery(broker, source);

    final XQueryContext context;
    if (compiled == null) {
      context = new XQueryContext(broker.getBrokerPool(), AccessContext.TEST);
      compiled = xqueryService.compile(broker, context, source);
    } else {
      context = compiled.getContext();
      compiled.getContext().updateContext(context);
      context.getWatchDog().reset();
    }

    try {
      return xqueryService.execute(broker, compiled, null, null);
    } finally {
      xqueryPool.returnCompiledXQuery(source, compiled);
    }
  }

  private String asXmlDoc(final String key, final Map<String, ByteIterator> values) {
    final StringBuilder builder = new StringBuilder();

    builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

    builder.append("<keys>");

    builder.append("<values key=\"");
    builder.append(key);
    builder.append("\">");
    valuesAsXml(builder, values);
    builder.append("</values>");

    builder.append("</keys>");

    return builder.toString();
  }

  private String asXmlElement(final String key, final Map<String, ByteIterator> values) {
    final StringBuilder builder = new StringBuilder();

    builder.append("<values key=\"");
    builder.append(key);
    builder.append("\">");
    valuesAsXml(builder, values);
    builder.append("</values>");

    return builder.toString();
  }

  private String asXmlDoc(final Map<String, ByteIterator> values) {
    final StringBuilder builder = new StringBuilder();

    builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    builder.append("<values>");
    valuesAsXml(builder, values);
    builder.append("</values>");

    return builder.toString();
  }

  private void valuesAsXml(final StringBuilder builder, final Map<String, ByteIterator> values) {
    for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
      builder.append("<value");

      builder.append(" key=\"");
      builder.append(value.getKey());
      builder.append("\"");

      builder.append(" value=\"");
      builder.append(new String(value.getValue().toArray(), Charsets.UTF_8));
      builder.append("\"");

      builder.append("/>");
    }
  }
}
