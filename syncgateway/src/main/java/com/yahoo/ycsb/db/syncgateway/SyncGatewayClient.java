/**
 * Copyright (c) 2016 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.syncgateway;


import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.*;


/**

 * <p> The following options can be passed when using this database restClient to override the defaults.
 *
 * <ul>
 * <li><b>syncgateway.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>syncgateway.db=db</b> Sync Gateway database name</li>
 * <li><b>syngateway.port.admin=4985</b> The admin port.</li>
 * <li><b>syngateway.port.public=4984</b> The public port.</li>
 * <li><b>syngateway.auth=false</b> If set to true YCSB will hit public port with user-level authorization.</li>
 * <li><b>syngateway.createusers=false</b> Create users instead od loading documents in 'loadl' phase</li>
 *
 * </ul>

 */

public class SyncGatewayClient extends DB {

  private Random rand = new Random();
  private MemcachedClient memcachedClient;

  /*  REST framework confoguration  */
  private static final String CON_TIMEOUT = "timeout.con";
  private static final String READ_TIMEOUT = "timeout.read";
  private static final String EXEC_TIMEOUT = "timeout.exec";
  private static final String LOG_ENABLED = "log.enable";
  private static final String HEADERS = "headers";
  private static final String COMPRESSED_RESPONSE = "response.compression";

  private boolean compressedResponse;
  private boolean logEnabled;
  private Properties props;
  private String[] headers;
  private CloseableHttpClient client;
  private int conTimeout = 10000;
  private int readTimeout = 10000;
  private int execTimeout = 10000;
  private volatile Criteria requestTimedout = new Criteria(false);

  /*  Sync Gateway test configuration */
  //connection
  private static final String SG_HOST = "syncgateway.host";
  private static final String SG_DB = "syncgateway.db";
  private static final String SG_PORT_ADMIN = "syncgateway.port.admin";
  private static final String SG_PORT_PUBLIC = "syncgateway.port.public";
  private static final String SG_AUTH = "syncgateway.auth";
  private static final String SG_DEFAULT_USER_PASSWORD = "password";

  private static final String MEMCACHED_HOST = "memcached.host";
  private static final String MEMCACHED_PORT = "memcached.port";

  //generic parameters
  private static final String SG_TOTAL_DOCUMENTS = "syncgateway.totaldocuments";
  private static final String SG_TOTAL_USERS = "syncgateway.totalusers";
  private static final String SG_TOTAL_CHANNELS = "syncgateway.channels";
  private static final String SG_CHANNELS_PER_USER = "syncgateway.channelsperuser";
  private static final String SG_CHANNELS_PER_DOCUMENT = "syncgateway.channelsperdocument";

  private static final String SG_DOCUMENTS_INSERTSTART = "syncgateway.documentsinsertstart";
  private static final String SG_USERS_INSERTSTART = "syncgateway.usersinsertstart";

  private static final String DEFAULT_USERNAME_PREFIX = "sg-user-";
  private static final String DEFAULT_CHANNEL_PREFIX = "channel-";

  //operation specific parameters
  private static final String SG_READ_OPERATION = "syncgateway.readoperation";
    private static final String SG_READ_OPERATION_DOCUMENTS = "documents";
    private static final String SG_READ_OPERATION_DOCUMENTS_WITH_REVS = "docswithrev";
    private static final String SG_READ_OPERATION_CHANGES  = "changes";

  private static final String SG_CHANGES_FEED = "syncgateway.changesfeed";
    private static final String SG_CHANGES_FEED_NORMAL = "normal";
    private static final String SG_CHANGES_FEED_LONGPOLL = "longpoll";

  private static final String SG_CHANGES_READ = "syncgateway.changesread";
    private static final String SG_CHANGES_READ_IDS = "ids";
    private static final String SG_CHANGES_READ_WITHDOCS = "withdoc";

  private static final String SG_CHANGES_SINCE = "syncgateway.changessince";
    private static final String SG_CHANGES_SINCE_RECENT = "recent";
    private static final String SG_CHANGES_SINCE_ZERO  = "zero";

  private static final String SG_ADHOC_OPERATION = "syncgateway.adhocoperation";
    private static final String SG_ADHOC_OPERATION_INITALLUSERS = "initallusers";
    private static final String SG_ADHOC_OPERATION_NONE = "none";

  private static final String SG_INSERT_OPERATION = "syncgateway.insertoperation";
    private static final String SG_INSERT_OPERATION_USER = "user";
    private static final String SG_INSERT_OPERATION_DOCUMENT = "document";
    private static final String SG_INSERT_OPERATION_DOCUMENT_ROUNDTRIP = "docroundtrip";

  private String portAdmin;
  private String portPublic;
  private boolean useAuth;
  private String[] hosts;
  private String host;

  private static CounterGenerator sgUsersCounter = new CounterGenerator(0);
  private static CounterGenerator sgChannelsCounter = new CounterGenerator(0);
  private static CounterGenerator sgRolesCounter = new CounterGenerator(0);
  private static CounterGenerator sgDocsCounter = new CounterGenerator(0);

  private String memcachedHost = props.getProperty(MEMCACHED_HOST, "localhost");
  private String memcachedPort = props.getProperty(MEMCACHED_PORT, "8000");
  private int sgTotalDocuments;
  private int sgTotalUsers;
  private int sgTotalChannels;
  private int sgChannelsPerUser;
  private int sgChannelsPerDocument;

  private String sgReadOperation;
  private String sgChangesFeed;
  private String sgChangesRead;
  private String sgChangesSince;
  private String sgAdhocOperation;
  private String sgInsertOperation;

  private int sgDocumentsInsertStart;
  private int sgUsersInsertStart;


  private String currentIterationUser;

  @Override
  public void init() throws DBException {
    props = getProperties();

    /* REST framework configuration and setup*/
    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
    compressedResponse = Boolean.valueOf(props.getProperty(COMPRESSED_RESPONSE, "false").trim());
    headers = props.getProperty(HEADERS, "Accept */* Content-Type application/xml user-agent Mozilla/5.0 ").trim()
        .split(" ");
    setupClient();


     /*  Sync Gateway test configuration */
    String hostParam = props.getProperty(SG_HOST, "127.0.0.1");
    hosts = hostParam.split(",");
    host = hosts[rand.nextInt(hosts.length)];
    String db = props.getProperty(SG_DB, "db");
    portAdmin = props.getProperty(SG_PORT_ADMIN, "4985");
    portPublic = props.getProperty(SG_PORT_PUBLIC, "4984");
    useAuth = props.getProperty(SG_AUTH, "false").equals("true");

    sgTotalDocuments = Integer.valueOf(props.getProperty(SG_TOTAL_DOCUMENTS, "1000"));
    sgTotalUsers = Integer.valueOf(props.getProperty(SG_TOTAL_USERS, "1000"));
    sgTotalChannels = Integer.valueOf(props.getProperty(SG_TOTAL_CHANNELS, "100"));
    sgChannelsPerUser = Integer.valueOf(props.getProperty(SG_CHANNELS_PER_USER, "10"));
    sgChannelsPerDocument = Integer.valueOf(props.getProperty(SG_CHANNELS_PER_DOCUMENT, "1"));

    sgReadOperation = props.getProperty(SG_READ_OPERATION, SG_READ_OPERATION_DOCUMENTS);
    sgChangesFeed = props.getProperty(SG_CHANGES_FEED, SG_CHANGES_FEED_NORMAL);
    sgChangesRead = props.getProperty(SG_CHANGES_READ, SG_CHANGES_READ_IDS);
    sgChangesSince = props.getProperty(SG_CHANGES_SINCE, SG_CHANGES_SINCE_ZERO);
    sgAdhocOperation = props.getProperty(SG_ADHOC_OPERATION, SG_ADHOC_OPERATION_NONE);
    sgInsertOperation = props.getProperty(SG_INSERT_OPERATION, SG_INSERT_OPERATION_DOCUMENT);

    sgDocumentsInsertStart = Integer.valueOf(props.getProperty(SG_DOCUMENTS_INSERTSTART, "0"));
    sgUsersInsertStart = Integer.valueOf(props.getProperty(SG_USERS_INSERTSTART, "0"));

    /* connect to local memcached */
    try {
      memcachedClient = createMemcachedClient(memcachedHost, Integer.parseInt(memcachedPort));
    } catch (Exception e) {
      System.err.println("Memcached init error" + e.getMessage());
      System.exit(1);
    }


    /* run adhoc operation if defined */
    if (!sgAdhocOperation.equals(SG_ADHOC_OPERATION_NONE)){
      try {
        if (sgAdhocOperation.equals(SG_ADHOC_OPERATION_INITALLUSERS)) {
          adhocOperationInitAllUsers();
        } else {
          throw new Exception("Not implemented");
        }
      } catch (Exception ex) {
        System.err.println("Adhoc operation " + sgAdhocOperation + "failed: " + ex.getMessage());
        System.exit(1);
      }
      System.out.println("Adhoc operation " + sgAdhocOperation + " completed, exiting");
      System.exit(0);
    }
  }



  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }


  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }


  private void adhocOperationInitAllUsers() {
    int userId = sgUsersCounter.nextValue() + sgUsersInsertStart;

    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode authDoc = factory.objectNode();
    authDoc.put("password", SG_DEFAULT_USER_PASSWORD);

    while (userId < sgTotalUsers) {
      String userName = DEFAULT_USERNAME_PREFIX + userId;
      Object storedSession = memcachedClient.get(userName);
      if (storedSession == null) {
        authDoc.put("name", userName);
        String requestBody = authDoc.toString();
        String sessionCookie = null;
        try {
          sessionCookie = httpAuthWithSessionCookie(requestBody);
          memcachedClient.set(userName, 0, sessionCookie);
        } catch (Exception e) {
          System.err.println("Autorization failure for user " + userName + ", exiting...");
          System.exit(1);
        }
      }
    }




    while (userId < (sgTotalUsers + sgUsersInsertStart)) {
      userId = sgUsersCounter.nextValue() + sgUsersInsertStart;
      if (userId < (sgTotalUsers + sgUsersInsertStart)) {
        String userName = DEFAULT_USERNAME_PREFIX + userId;
        Object storedSession = memcachedClient.get(userName);
        if (storedSession == null) {
          authDoc.put("name", userName);
          String requestBody = authDoc.toString();
          String sessionCookie = null;







          try {
            sessionCookie = httpAuthWithSessionCookie(requestBody);
            memcachedClient.set(userName, 0, sessionCookie);
          } catch (Exception e) {
            System.err.println("Autorization failure for user " + userName + ", exiting...");
            System.exit(1);
          }
        }
        setLocalSequenceForUser(userName, sequencestart);
      }
    }
    setLocalSequenceGlobally(sequencestart);
  }



  private CloseableHttpResponse httpGet(String endpoint){
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    if (useAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    return = client.execute(request);
  }


  private String getSessionCookieByUser(String userName) throws IOException {
    String sessionCookie =  memcachedClient.get(currentIterationUser).toString();
    if (sessionCookie == null) {
      throw new IOException("No session cookie stored for user + " + userName);
    }
    return sessionCookie;
  }

  private void setupClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
    this.client = clientBuilder.setConnectionManagerShared(true).build();
  }


  private net.spy.memcached.MemcachedClient createMemcachedClient(String memHost, int memPort)
      throws Exception {
    String address = memHost + ":" + memPort;

    return new net.spy.memcached.MemcachedClient(
        new net.spy.memcached.ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),
        net.spy.memcached.AddrUtil.getAddresses(address));
  }

  /**
   * Marks the input {@link Criteria} as satisfied when the input time has elapsed.
   */
  class Timer implements Runnable {

    private long timeout;
    private Criteria timedout;

    public Timer(long timeout, Criteria timedout) {
      this.timedout = timedout;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
        this.timedout.setIsSatisfied(true);
      } catch (InterruptedException e) {
        // Do nothing.
      }
    }

  }


  /**
   * Sets the flag when a criteria is fulfilled.
   */
  class Criteria {

    private boolean isSatisfied;

    public Criteria(boolean isSatisfied) {
      this.isSatisfied = isSatisfied;
    }

    public boolean isSatisfied() {
      return isSatisfied;
    }

    public void setIsSatisfied(boolean satisfied) {
      this.isSatisfied = satisfied;
    }

  }

  /**
   * Private exception class for execution timeout.
   */
  class TimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TimeoutException() {
      super("HTTP Request exceeded execution time limit.");
    }

  }


}
