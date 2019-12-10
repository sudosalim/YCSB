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


import com.fasterxml.jackson.databind.node.ArrayNode;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.*;
import java.util.*;


/**
 TODO: Summary

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
  private static CounterGenerator sgUserInsertCounter = new CounterGenerator(0);
  private static CounterGenerator sgUsersPool = new CounterGenerator(0);
  private static CounterGenerator sgAccessPool = new CounterGenerator(0);
  private static final String HTTP_CON_TIMEOUT = "rest.timeout.con";
  private static final String HTTP_READ_TIMEOUT = "rest.timeout.read";
  private static final String HTTP_EXEC_TIMEOUT = "rest.timeout.exec";
  private static final String HTTP_HEADERS = "headers";

  private static final String SG_HOST = "syncgateway.host";
  private static final String SG_DB = "syncgateway.db";
  private static final String SG_PORT_ADMIN = "syncgateway.port.admin";
  private static final String SG_PORT_PUBLIC = "syncgateway.port.public";
  private static final String SG_AUTH = "syncgateway.auth";
  private static final String SG_BASIC_AUTH = "syncgateway.basic_auth";
  private static final String SG_STAR_CHANNEL = "syncgateway.starchannel";
  private static final String SG_LOAD_MODE = "syncgateway.loadmode";
  private static final String SG_READ_MODE = "syncgateway.readmode";
  private static final String SG_INSERT_MODE = "syncgateway.insertmode";
  private static final String SG_ROUND_TRIP_WRITE = "syncgateway.roundtrip";
  private static final String SG_TOTAL_USERS = "syncgateway.totalusers";
  private static final String SG_TOTAL_CHANNELS = "syncgateway.channels";
  private static final String SG_CHANNELS_PER_USER = "syncgateway.channelsperuser";
  private static final String SG_CHANNELS_PER_DOCUMENT = "syncgateway.channelsperdocument";

  private static final String SG_SEQUENCE_START = "syncgateway.sequencestart";
  private static final String SG_INSERTUSERS_START = "syncgateway.insertusersstart";
  private static final String SG_FEED_MODE = "syncgateway.feedmode";
  private static final String SG_FEED_READING_MODE = "syncgateway.feedreadingmode";

  private static final String SG_INIT_USERS = "syncgateway.initusers";

  private static final String MEMCACHED_HOST = "memcached.host";
  private static final String MEMCACHED_PORT = "memcached.port";

  private static final String DEFAULT_USERNAME_PREFIX = "sg-user-";
  private static final String DEFAULT_CHANNEL_PREFIX = "channel-";
  private static final String DEFAULT_USER_PASSWORD = "password";

  private static final int SG_LOAD_MODE_USERS = 0;
  private static final int SG_LOAD_MODE_DOCUMENTS = 1;

  private static final int SG_INSERT_MODE_BYKEY = 0;
  private static final int SG_INSERT_MODE_BYUSER = 1;

  private static final int SG_READ_MODE_DOCUMENTS = 0;
  private static final int SG_READ_MODE_DOCUMENTS_WITH_REV = 1;
  private static final String SG_REPLICATOR2 = "syncgateway.replicator2";

  private static final int SG_READ_MODE_CHANGES = 2;
  private static final int SG_READ_MODE_ALLCHANGES = 3;
  private static final int SG_READ_MODE_200CHANGES = 4;

  private static final String SG_FEED_READ_MODE_IDSONLY = "idsonly";
  private static final String SG_FEED_READ_MODE_WITHDOCS = "withdocs";

  private static final String SG_CHANNELS_PER_GRANT = "syncgateway.channelspergrant";
  private static final String SG_GRANT_ACCESS_TO_ALL_USERS = "syncgateway.grantaccesstoall";
  private static final String SG_GRANT_ACCESS_IN_SCAN = "syncgateway.grantaccessinscan";

  // Sync Gateway parameters
  private String portAdmin;
  private String portPublic;
  private boolean useAuth;
  private boolean basicAuth;
  private boolean roudTripWrite;
  private int loadMode;
  private int readMode;
  private boolean isSgReplicator2;
  private int totalUsers;
  private int totalChannels;
  private int channelsPerUser;
  private int channelsPerDocument;
  private String[] hosts;
  private String host;
  private int insertMode;
  private String sequencestart;
  private boolean initUsers;
  private int insertUsersStart = 0;
  private String feedMode;
  private boolean includeDocWhenReadingFeed;
  private boolean starChannel;
  private int channelsPerGrant;
  private boolean grantAccessToAllUsers;
  private boolean grantAccessInScanOperation;


  // http parameters
  private volatile Criteria requestTimedout = new Criteria(false);
  private String[] headers;
  private int conTimeout = 5000;
  private int readTimeout = 5000;
  private int execTimeout = 5000;
  private CloseableHttpClient restClient;


  //memcached
  private MemcachedClient memcachedClient;

  //private String urlEndpointPrefix;
  private String createUserEndpoint;
  private String documentEndpoint;
  private String createSessionEndpoint;

  private String currentIterationUser = null;
  private Random rand = new Random();


  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String hostParam = props.getProperty(SG_HOST, "127.0.0.1");
    hosts = hostParam.split(",");
    host = hosts[rand.nextInt(hosts.length)];

    String db = props.getProperty(SG_DB, "db");
    portAdmin = props.getProperty(SG_PORT_ADMIN, "4985");
    portPublic = props.getProperty(SG_PORT_PUBLIC, "4984");
    useAuth = props.getProperty(SG_AUTH, "false").equals("true");
    basicAuth = props.getProperty(SG_BASIC_AUTH, "false").equals("true");
    starChannel = props.getProperty(SG_STAR_CHANNEL, "false").equals("true");
    loadMode = (props.getProperty(SG_LOAD_MODE, "documents").equals("users")) ?
        SG_LOAD_MODE_USERS : SG_LOAD_MODE_DOCUMENTS;

    insertMode = (props.getProperty(SG_INSERT_MODE, "bykey").equals("bykey")) ?
        SG_INSERT_MODE_BYKEY : SG_INSERT_MODE_BYUSER;

    String runModeProp = props.getProperty(SG_READ_MODE, "documents");
    if (runModeProp.equals("documents")) {
      readMode = SG_READ_MODE_DOCUMENTS;
    } else if (runModeProp.equals("changes")) {
      readMode = SG_READ_MODE_CHANGES;
    } else if (runModeProp.equals("allchanges")) {
      readMode = SG_READ_MODE_ALLCHANGES;
    } else if (runModeProp.equals("200changes")) {
      readMode = SG_READ_MODE_200CHANGES;
    } else {
      readMode = SG_READ_MODE_DOCUMENTS_WITH_REV;
    }

    totalUsers = Integer.valueOf(props.getProperty(SG_TOTAL_USERS, "1000"));
    totalChannels = Integer.valueOf(props.getProperty(SG_TOTAL_CHANNELS, "100"));
    roudTripWrite = props.getProperty(SG_ROUND_TRIP_WRITE, "false").equals("true");
    isSgReplicator2 = props.getProperty(SG_REPLICATOR2, "false").equals("true");
    initUsers = props.getProperty(SG_INIT_USERS, "true").equals("true");
    channelsPerUser = Integer.valueOf(props.getProperty(SG_CHANNELS_PER_USER, "10"));
    channelsPerDocument = Integer.valueOf(props.getProperty(SG_CHANNELS_PER_DOCUMENT, "1"));

    feedMode = props.getProperty(SG_FEED_MODE, "normal");
    if (!feedMode.equals("normal") && (!feedMode.equals("longpoll"))) {
      feedMode = "normal";
    }

    String feedReadingModeProp = props.getProperty(SG_FEED_READING_MODE, SG_FEED_READ_MODE_IDSONLY);

    includeDocWhenReadingFeed = feedReadingModeProp.equals(SG_FEED_READ_MODE_WITHDOCS);

    insertUsersStart = Integer.valueOf(props.getProperty(SG_INSERTUSERS_START, "0"));
    conTimeout = Integer.valueOf(props.getProperty(HTTP_CON_TIMEOUT, "1")) * conTimeout;
    readTimeout = Integer.valueOf(props.getProperty(HTTP_READ_TIMEOUT, "1")) * readTimeout;
    execTimeout = Integer.valueOf(props.getProperty(HTTP_EXEC_TIMEOUT, "1")) * execTimeout;
    headers = props.getProperty(HTTP_HEADERS, "Accept */* Content-Type application/json user-agent Mozilla/5.0 ").
        trim().split(" ");

    String memcachedHost = props.getProperty(MEMCACHED_HOST, "localhost");
    String memcachedPort = props.getProperty(MEMCACHED_PORT, "8000");

    sequencestart = props.getProperty(SG_SEQUENCE_START, "2000001");

    channelsPerGrant = Integer.parseInt(props.getProperty(SG_CHANNELS_PER_GRANT, "1"));
    grantAccessToAllUsers = props.getProperty(SG_GRANT_ACCESS_TO_ALL_USERS, "false").equals("true");
    grantAccessInScanOperation = props.getProperty(SG_GRANT_ACCESS_IN_SCAN, "false").equals("true");

    createUserEndpoint = "/" + db + "/_user/";
    documentEndpoint = "/" + db + "/";
    createSessionEndpoint = "/" + db + "/_session";

    // Init components
    restClient = createRestClient();

    try {
      memcachedClient = createMemcachedClient(memcachedHost, Integer.parseInt(memcachedPort));
    } catch (Exception e) {
      System.err.println("Memcached init error" + e.getMessage());
      System.exit(1);
    }

    if ((loadMode != SG_LOAD_MODE_USERS) && (useAuth) && (initUsers)) {
      initAllUsers();
    }

    if (grantAccessToAllUsers) {
      grantAccessToAllUsers();
    }

  }


  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

    assignRandomUserToCurrentIteration();

    if (readMode == SG_READ_MODE_CHANGES) {
      return readChanges(key);
    } else if (readMode == SG_READ_MODE_ALLCHANGES) {
      return readAllChanges(key);
    } else if (readMode == SG_READ_MODE_200CHANGES) {
      return read200Changes(key);
    }


    return readSingle(key, result);
  }

  private Status readChanges(String key) {
    try {
      String seq = getLocalSequenceForUser(currentIterationUser);
      checkForChanges(seq, getChannelNameByKey(key));
    } catch (Exception e) {
      return Status.ERROR;
    }
    syncronizeSequencesForUser(currentIterationUser);

    return Status.OK;
  }


  private Status read200Changes(String key) {
    try {
      String seq = getLocalSequenceForUser(currentIterationUser);
      seq = String.valueOf(Integer.parseInt(seq) - 200);
      checkForChanges(seq, getChannelNameByKey(key));
    } catch (Exception e) {
      return Status.ERROR;
    }
    syncronizeSequencesForUser(currentIterationUser);

    return Status.OK;
  }

  private Status readAllChanges(String key) {
    try {
      checkForChanges("0", getChannelNameByKey(key));
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Status readSingle(String key, HashMap<String, ByteIterator> result) {
    String port = (useAuth) ? portPublic : portAdmin;

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + key;

    if (readMode == SG_READ_MODE_DOCUMENTS_WITH_REV && !isSgReplicator2) {

      String revisionID = getRevision(key);
      if (revisionID == null){
        System.out.println("RevisionID not found for key :" + key);
      }
      fullUrl += "?rev=" + revisionID;
    }

    if(isSgReplicator2 && readMode == SG_READ_MODE_DOCUMENTS_WITH_REV){

      String revisionID = getRevision(key);
      if (revisionID == null){
        System.out.println("RevisionID not found for key :" + key);
      }
      fullUrl += "?rev=" + revisionID;
      fullUrl += "&replicator2=true";
    }

    if(isSgReplicator2 && readMode != SG_READ_MODE_DOCUMENTS_WITH_REV){
      fullUrl += "?replicator2=true";
    }


    int responseCode;
    try {
      responseCode = httpGet(fullUrl, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "GET");
    }
    return getStatus(responseCode);
  }


  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    assignRandomUserToCurrentIteration();
    if (grantAccessInScanOperation) {
      insertAccessGrant(currentIterationUser);
    }
    return authRandomUser();

    //return Status.NOT_IMPLEMENTED;
  }


  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    assignRandomUserToCurrentIteration();

    String requestBody = buildDocumentFromMap(key, values);
    String docRevision = getRevision(key);

    if (docRevision == null) {
      System.err.println("Revision for document " + key + " not found in local");
      return Status.UNEXPECTED_STATE;
    }

    String currentSequence = getLocalSequenceForUser(currentIterationUser);
    String port = (useAuth) ? portPublic : portAdmin;
    String fullUrl;
    int responseCode;

    fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + key + "?rev=" + docRevision;

    if(isSgReplicator2){
      fullUrl += "&replicator2=true";
    }

    HttpPut httpPutRequest = new HttpPut(fullUrl);

    try {
      responseCode = httpExecute(httpPutRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "PUT");
    }

    Status result = getStatus(responseCode);
    if (result == Status.OK) {
      incrementLocalSequenceForUser();
      if (roudTripWrite) {
        if ((currentSequence == null) || (currentSequence.equals(""))) {
          System.err.println("Memcached failure!");
          return Status.BAD_REQUEST;
        }
        try {
          waitForDocInChangeFeed(currentSequence, key);
        } catch (Exception e) {
          syncronizeSequencesForUser(currentIterationUser);
          return Status.UNEXPECTED_STATE;
        }
      }
    }
    return result;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (loadMode == SG_LOAD_MODE_USERS) {
      return insertUser(table, key, values);
    }

    assignRandomUserToCurrentIteration();
    return insertDocument(table, key, values);

  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private Status authRandomUser() {

    String requestBody = buildAutorizationBody(currentIterationUser);
    try {
      httpAuthWithSessionCookie(requestBody);
    } catch (IOException ex) {
      return Status.ERROR;
    }
    return Status.OK;
  }


  private Status insertUser(String table, String key, HashMap<String, ByteIterator> values) {

    String requestBody = buildUserDef();
    String fullUrl = "http://" + getRandomHost() + ":" + portAdmin + createUserEndpoint;
    HttpPost httpPostRequest = new HttpPost(fullUrl);

    int responseCode;
    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }
    Status result = getStatus(responseCode);
    if (result == Status.OK) {
      incrementLocalSequenceForUser();
    }

    return result;
  }


  private Status insertDocument(String table, String key, HashMap<String, ByteIterator> values) {

    String port = (useAuth) ? portPublic : portAdmin;
    String requestBody;
    String fullUrl;

    String channel = null;

    if (insertMode == SG_INSERT_MODE_BYKEY) {
      channel = getChannelNameByTotalChannels();
      //requestBody = buildDocumentWithChannel(key, values, channel);
    } else {
      channel = getChannelForUser();
      //requestBody = buildDocumentFromMap(key, values);
    }

    requestBody = buildDocumentWithChannel(key, values, channel);

    fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint;

    if(isSgReplicator2){
      fullUrl += "?replicator2=true";
    }

    String currentSequence = getLocalSequenceGlobal();
    String lastSequence = getLastSequenceGlobal();
    //System.out.println("Printing it here before saving" + lastSequence);
    //System.out.println("Printing it for key " + key);

    String lastseq = null;

    HttpPost httpPostRequest = new HttpPost(fullUrl);
    int responseCode;
    //System.out.println("before INSERT: User " + currentIterationUser + " just inserted doc " + key
    //+ " SG seq before insert was " + currentSequence);

    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
      //System.out.println("printing the responseCode " + responseCode);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }

    //System.out.println("INSERT: User " + currentIterationUser + " just inserted doc " + key
    //+ " SG seq before insert was " + currentSequence);

    Status result = getStatus(responseCode);
    if (result == Status.OK) {
      incrementLocalSequenceForUser();
      if (roudTripWrite) {

        if ((lastSequence == null) || (lastSequence.equals(""))) {
          System.err.println("Memcached failure!");
          return Status.BAD_REQUEST;
        }
        try {
          //System.out.println("entering waitForDocInChangeFeed3 since its SG_INSERT_MODE_BYKEY " + channel);
          if (feedMode.equals("longpoll")){
            lastseq = waitForDocInChangeFeed4(lastSequence, channel, key);
          } else {
            lastseq = waitForDocInChangeFeed3(lastSequence, channel, key);
          }
          //System.out.println("chanel and last seq " + lastseq);
          //System.out.println("lastseq from waitForDocInChangeFeed2" + lastseq);
          incrementLocalSequenceGlobal();
          setLastSequenceGlobally(lastseq);
        } catch (Exception e) {
          System.err.println("Failed to sync lastSeq value  | lastseq : "
              + lastseq + " | fullUrl : " + fullUrl + " | lastSequence :"
              + lastSequence + " | key: " + key + " | channel" + channel);
          syncronizeSequencesForUser(currentIterationUser);
          return Status.UNEXPECTED_STATE;
        }
      }

    }

    return result;
  }


  private Status insertAccessGrant(String userName) {

    String port = (useAuth) ? portPublic : portAdmin;
    String requestBody;
    String fullUrl;

    requestBody = buildAccessGrantDocument(userName);

    fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint;

    HttpPost httpPostRequest = new HttpPost(fullUrl);
    int responseCode;

    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }
    Status result = getStatus(responseCode);

    return result;
  }

  private String buildAccessGrantDocument(String userName) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    String agKey = "accessgrant_" + userName;
    root.put("_id", agKey);
    String accessFieldName = "access";
    String accessToFieldName = "accessTo";
    root.put(accessToFieldName, userName);
    if (channelsPerGrant == 1) {
      root.put(accessFieldName, DEFAULT_CHANNEL_PREFIX + rand.nextInt(totalChannels));
    } else {
      ArrayNode usersNode = factory.arrayNode();
      for (int i = 0; i < channelsPerGrant; i++) {
        usersNode.add(DEFAULT_CHANNEL_PREFIX + rand.nextInt(totalChannels));
      }
      root.set(accessFieldName, usersNode);
    }
    return root.toString();
  }


  private void checkForChanges(String sequenceSince, String channel) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String includeDocsParam = "include_docs=false";
    if (includeDocWhenReadingFeed) {
      includeDocsParam = "include_docs=true";
    }

    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=normal&" + includeDocsParam;
    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

    //System.out.println(fullUrl);

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    if (useAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    CloseableHttpResponse response = restClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      //String fullResponse = "CHANGES FEED RESPONSE: " + "(user)" + currentIterationUser + ", (changes since) " +
      //    sequenceSince + ": ";
      while ((line = reader.readLine()) != null) {
        //fullResponse = fullResponse + line + "\n";
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! for " + changesFeedEndpoint);
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      //System.out.println(fullResponse);
      timer.interrupt();
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();
  }


  private String waitForDocInChangeFeed2(String sequenceSince, String key) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=" + feedMode +
        "&filter=sync_gateway/bychannel&channels=" + getChannelForUser();

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

    //System.out.println("Printing fullUrl" + fullUrl);

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    if (useAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    CloseableHttpResponse response = null;
    boolean docFound = false;
    String lastseq = null;

    int repeatCounter = 1000;
    while (!docFound) {
      repeatCounter--;
      if (repeatCounter <= 0) {
        response.close();
        restClient.close();
        System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! by repeatCounter for " + changesFeedEndpoint);
        throw new TimeoutException();
      }
      try {
        response = restClient.execute(request);
      } catch (java.net.SocketTimeoutException ex) {
        response.close();
        restClient.close();
        System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! by Socket ex for " + changesFeedEndpoint
            + " " + ex.getStackTrace());
        throw new TimeoutException();
      }
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        InputStream stream = responseEntity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuffer responseContent = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
          if (requestTimedout.isSatisfied()) {
            // Must avoid memory leak.
            reader.close();
            stream.close();
            EntityUtils.consumeQuietly(responseEntity);
            response.close();
            restClient.close();
            throw new TimeoutException();
          }
          if (lookForDocID(line, key)) {
            docFound = true;
            String[] arrOfstr = line.split(":", 2);
            String[] arrOfstr2 = arrOfstr[1].split(",", 2);
            lastseq = arrOfstr2[0];
          }
          responseContent.append(line);
        }
        timer.interrupt();
        stream.close();
      }
      if (requestTimedout.isSatisfied()) {
        EntityUtils.consumeQuietly(responseEntity);
        response.close();
        restClient.close();
        throw new TimeoutException();
      }
      EntityUtils.consumeQuietly(responseEntity);
      response.close();
    }
    restClient.close();

    return lastseq;
  }


  private String waitForDocInChangeFeed3(String sequenceSince, String channel, String key) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=" + feedMode +
        "&filter=sync_gateway/bychannel&channels=" + channel;

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;


    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    if (useAuth && !basicAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    if (basicAuth) {
      String auth = currentIterationUser + ":" + DEFAULT_USER_PASSWORD;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
      String authHeader = "Basic " + new String(encodedAuth);
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }


    CloseableHttpResponse response = null;
    boolean docFound = false;
    String lastseq = null;

    int repeatCounter = 1000;
    while (!docFound) {

      repeatCounter--;
      if (repeatCounter <= 0) {
        response.close();
        restClient.close();
        System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! by repeatCounter for " + changesFeedEndpoint);
        throw new TimeoutException();
      }
      try {
        response = restClient.execute(request);
      } catch (java.net.SocketTimeoutException ex) {
        response.close();
        restClient.close();
        System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! by Socket ex for " + changesFeedEndpoint
            + " " + ex.getStackTrace());
        throw new TimeoutException();
      }
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        InputStream stream = responseEntity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuffer responseContent = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
          if (requestTimedout.isSatisfied()) {
            // Must avoid memory leak.
            reader.close();
            stream.close();
            EntityUtils.consumeQuietly(responseEntity);
            response.close();
            restClient.close();
            throw new TimeoutException();
          }
          if (lookForDocID(line, key)) {
            docFound = true;
            String[] arrOfstr = line.split(":", 2);
            String[] arrOfstr2 = arrOfstr[1].split(",", 2);
            lastseq = arrOfstr2[0];
          }
          responseContent.append(line);
        }
        timer.interrupt();
        stream.close();
      }
      if (requestTimedout.isSatisfied()) {
        EntityUtils.consumeQuietly(responseEntity);
        response.close();
        restClient.close();
        throw new TimeoutException();
      }
      EntityUtils.consumeQuietly(responseEntity);
      response.close();
    }
    restClient.close();

    return lastseq;
  }


  private String waitForDocInChangeFeed4(String sequenceSince, String channel, String key) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=" + feedMode +
        "&filter=sync_gateway/bychannel&channels=" + channel;

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    if (useAuth && !basicAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    if (basicAuth) {
      String auth = currentIterationUser + ":" + DEFAULT_USER_PASSWORD;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
      String authHeader = "Basic " + new String(encodedAuth);
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    CloseableHttpResponse response = null;
    String lastseq = null;

    long startTime = System.nanoTime();

    try {
      response = restClient.execute(request);
    } catch (Exception e) {
      System.err.println("_change Request Failed with exception " + e);
      response.close();
      restClient.close();
      return lastseq;
      //System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! by Socket ex for " + changesFeedEndpoint
      //    + " " + ex.getStackTrace());
      //throw new TimeoutException();
    }

    long endTime = System.nanoTime();

    boolean docFound = false;

    int responseCode = response.getStatusLine().getStatusCode();
    if(responseCode != 200){
      System.err.println("responseCode not 200 for _changes request :"
          + request + " | response :" + response);
    }

    HttpEntity responseEntity = response.getEntity();

    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (!docFound){
          if (requestTimedout.isSatisfied()) {
            long timetaken =  endTime - startTime;
            System.err.println("change request timed out | request : " + request +
                " | response :" + response + " | responseContent :"
                + responseContent + " | line : " + line + " | start time :" + startTime
                + " | endTime: " + endTime +  "  | time taken : " +  timetaken);

            // Must avoid memory leak.
            reader.close();
            stream.close();
            EntityUtils.consumeQuietly(responseEntity);
            response.close();
            restClient.close();
            throw new TimeoutException();
          }

          if (lookForDocID(line, key)) {
            docFound = true;
          }
        }

        if(line.contains("last_seq") && docFound){
          String[] arrOfstr = line.split(":", 2);
          String[] arrOfstr2 = arrOfstr[1].split("\"");
          lastseq = arrOfstr2[1];

        }
        responseContent.append(line);
      }
      if(!docFound){
        System.err.println("doc not found for this _change request :"
            + request + " | responseContent:" + responseContent + " | channel:"
            + channel + " | looking for key:" + key);
      }
      timer.interrupt();
      stream.close();
    }

    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();

    return lastseq;
  }


  private void waitForDocInChangeFeed(String sequenceSince, String key) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=" + feedMode +
        "&filter=sync_gateway/bychannel&channels=" + getChannelForUser();

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    if (useAuth && !basicAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    if (basicAuth) {
      String auth = currentIterationUser + ":" + DEFAULT_USER_PASSWORD;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
      String authHeader = "Basic " + new String(encodedAuth);
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    int counter = 10;
    boolean docFound = false;
    while (!docFound) {
      counter--;
      if (counter < 0) {
        requestTimedout.isSatisfied = true;
        System.err.println(" -= waitForDocInChangeFeed -= COUNTER OUT for " + changesFeedEndpoint);
        docFound = true;
      }
      CloseableHttpResponse response = restClient.execute(request);
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        InputStream stream = responseEntity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuffer responseContent = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
          //System.out.println(line);
          if (lookForDocID(line, key)) {
            docFound = true;
          }
          //docFound = true;
          if (requestTimedout.isSatisfied()) {
            // Must avoid memory leak.
            reader.close();
            stream.close();
            EntityUtils.consumeQuietly(responseEntity);
            response.close();
            restClient.close();
            System.err.println(" -= waitForDocInChangeFeed -= TIMEOUT! for " + changesFeedEndpoint);
            throw new TimeoutException();
          }
          responseContent.append(line);
        }
        timer.interrupt();
        // Closing the input stream will trigger connection release.
        stream.close();
      }
      EntityUtils.consumeQuietly(responseEntity);
      response.close();
    }
    restClient.close();
  }


  private int httpExecute(HttpEntityEnclosingRequestBase request, String data)
      throws IOException {


    if (basicAuth) {
      String auth = currentIterationUser + ":" + DEFAULT_USER_PASSWORD;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
      String authHeader = "Basic " + new String(encodedAuth);
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    if (useAuth && !basicAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data.getBytes()),
        ContentType.APPLICATION_FORM_URLENCODED);
    reqEntity.setChunked(true);
    request.setEntity(reqEntity);
    long startTime = System.nanoTime();
    CloseableHttpResponse response = restClient.execute(request);
    long endTime = System.nanoTime();
    responseCode = response.getStatusLine().getStatusCode();
    //System.err.println("printing response code for all post requests" + responseCode);
    if (responseCode != 200){
      //System.err.println("Doc Insert failed for request :" + request);
      //System.err.println("Printing response message if responseCode not 200 :" + response);
    }
    HttpEntity responseEntity = response.getEntity();
    boolean responseGenericValidation = true;
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        //System.err.println("repsonse line :" + line);
        storeRevisions(line, currentIterationUser);
        responseGenericValidation = validateHttpResponse(line);
        if (requestTimedout.isSatisfied()) {

          long timetaken = endTime - startTime;

          System.err.println("request timing out | request : " + request +
              " | response :" + response + " | responseContent :"
              + responseContent + " | line : " + line + " | start time :" + startTime
              + " | endTime: " + endTime +  "  | time taken : " +  timetaken);
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();

    if (!responseGenericValidation) {
      return 500;
    }

    return responseCode;
  }


  // Connection is automatically released back in case of an exception.
  private int httpGet(String endpoint, HashMap<String, ByteIterator> result) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    //int responseCode = 200;

    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    if (basicAuth) {
      String auth = currentIterationUser + ":" + DEFAULT_USER_PASSWORD;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
      String authHeader = "Basic " + new String(encodedAuth);
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    if (useAuth && !basicAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    CloseableHttpResponse response = restClient.execute(request);

    int responseCode = response.getStatusLine().getStatusCode();

    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      result.put("response", new StringByteIterator(responseContent.toString()));
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();
    return responseCode;
  }


  private String httpAuthWithSessionCookie(String data) throws IOException {

    String fullUrl = "http://" + getRandomHost() + ":" + portAdmin + createSessionEndpoint;
    HttpPost request = new HttpPost(fullUrl);

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();

    String sessionCookie = null;

    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data.getBytes()),
        ContentType.APPLICATION_FORM_URLENCODED);
    reqEntity.setChunked(true);
    request.setEntity(reqEntity);
    CloseableHttpResponse response = restClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (sessionCookie == null) {
          sessionCookie = readSessionCookie(line);
        }
        if (requestTimedout.isSatisfied()) {
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          throw new TimeoutException();
        }
      }
      timer.interrupt();
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();
    if (sessionCookie == null) {
      throw new IOException("No session cookie received from server");
    }
    return sessionCookie;
  }


  private int handleExceptions(Exception e, String url, String method) {
    System.err.println(new StringBuilder(method).append(" Request: ").append(url).append(" | ")
        .append(e.getClass().getName()).append(" occured | Error message: ")
        .append(e.getMessage()).toString());
    return 500;
  }

  // Maps HTTP status codes to YCSB status codes.
  private Status getStatus(int responseCode) {
    int rc = responseCode / 100;
    if (responseCode == 400) {
      return Status.BAD_REQUEST;
    } else if (responseCode == 401) {
      return Status.LOGIN_REQUIRED;
    } else if (responseCode == 403) {
      return Status.FORBIDDEN;
    } else if (responseCode == 404) {
      return Status.NOT_FOUND;
    } else if (responseCode == 501) {
      return Status.NOT_IMPLEMENTED;
    } else if (responseCode == 503) {
      return Status.SERVICE_UNAVAILABLE;
    } else if (rc == 5) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private String getRandomHost() {
    //hosts[rand.nextInt(hosts.length)];
    return host;
  }

  private String buildUserDef() {
    int id = sgUserInsertCounter.nextValue();
    String userName = DEFAULT_USERNAME_PREFIX + id;
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    root.put("name", userName);
    root.put("password", DEFAULT_USER_PASSWORD);
    ArrayNode channels = factory.arrayNode();

    if (starChannel) {
      channels.add("*");
      saveChannelForUser(userName, "*");
    } else if ((totalChannels == totalUsers) && (channelsPerUser == 1)){
      String channelName = DEFAULT_CHANNEL_PREFIX + id;
      channels.add(channelName);
      saveChannelForUser(userName, channelName);
    } else {
      String[] channelsSet = getSetOfRandomChannels();
      for (int i = 0; i < channelsPerUser; i++) {
        channels.add(channelsSet[i]);
      }
      saveChannelForUser(userName, channelsSet[0]);
    }
    root.set("admin_channels", channels);
    return root.toString();
  }

  private String buildDocumentFromMap(String key, HashMap<String, ByteIterator> values) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    ArrayNode channelsNode = factory.arrayNode();
    root.put("_id", key);

    if (insertMode == SG_INSERT_MODE_BYKEY) {
      channelsNode.add(getChannelNameByKey(key));
    } else {
      channelsNode.add(getChannelForUser());
    }

    root.set("channels", channelsNode);

    values.forEach((k, v) -> {
        root.put(k, v.toString());
      });
    return root.toString();
  }

  private String buildDocumentWithChannel(String key, HashMap<String, ByteIterator> values, String channel) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    ArrayNode channelsNode = factory.arrayNode();
    root.put("_id", key);

    channelsNode.add(channel);

    root.set("channels", channelsNode);

    values.forEach((k, v) -> {
        root.put(k, v.toString());
      });
    return root.toString();
  }

  private String getChannelNameByKey(String key) {
    int channelId = Math.abs(key.hashCode() % totalChannels);
    return DEFAULT_CHANNEL_PREFIX + channelId;
  }

  private String getChannelNameByTotalChannels() {
    int channelId = (int)(Math.random() * totalChannels) + 1;
    return DEFAULT_CHANNEL_PREFIX + channelId;
  }

  private String[] getSetOfRandomChannels() {

    String[] channels = new String[channelsPerUser];
    int[] allChannels = new int[totalChannels];

    for (int i = 0; i < totalChannels; i++) {
      allChannels[i] = i;
    }
    shuffleArray(allChannels);
    for (int i = 0; i < channelsPerUser; i++) {
      channels[i] = DEFAULT_CHANNEL_PREFIX + allChannels[i];
    }
    return channels;
  }

  private void shuffleArray(int[] array) {
    int index;
    //Random random = new Random();
    for (int i = array.length - 1; i > 0; i--) {
      index = rand.nextInt(i + 1);
      if (index != i) {
        array[index] ^= array[i];
        array[i] ^= array[index];
        array[index] ^= array[i];
      }
    }
  }

  private void saveChannelForUser(String userName, String channelName) {
    memcachedClient.set("_channel_" + userName, 0, channelName);
  }

  private String getChannelForUser() {
    return memcachedClient.get("_channel_" + currentIterationUser).toString();
  }

  private void setLocalSequenceForUser(String userName, String seq) {
    memcachedClient.set("_sequenceCounter_" + userName, 0, seq);
  }

  private void setLocalSequenceGlobally(String seq) {
    memcachedClient.set("_sequenceCounterGlobal", 0, seq);
  }

  private String getLocalSequenceGlobal() {
    Object localSegObj = memcachedClient.get("_sequenceCounterGlobal");
    if (localSegObj == null) {
      setLocalSequenceGlobally(sequencestart);
      return memcachedClient.get("_sequenceCounterGlobal").toString();
    }
    return localSegObj.toString();
  }

  private void setLastSequenceGlobally(String seq) {
    memcachedClient.set("_lastsequenceCounterGlobal", 0, seq);
  }

  private String getLastSequenceGlobal() {
    Object localSegObj = memcachedClient.get("_lastsequenceCounterGlobal");
    String lastseq = null;
    if (localSegObj == null) {
      //System.out.println("entered this since its localseqobj is zero");
      try {
        lastseq = getlastSequenceFromSG();
      } catch (Exception e) {
        System.err.println(e);
      }
      setLastSequenceGlobally(lastseq);
      return memcachedClient.get("_lastsequenceCounterGlobal").toString();
    }
    //System.out.println("printing last sequence from getLastSequenceGlobal" + localSegObj.toString());
    return localSegObj.toString();
  }

  private String getlastSequenceFromSG() throws IOException {
    String port = portAdmin;

    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint;

    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet(fullUrl);
    //for (int i = 0; i < headers.length; i = i + 2) {
    //  request.setHeader(headers[i], headers[i + 1]);
    //}

    String lastsequence = null;
    //String str1 = null;
    int counter = 10;

    CloseableHttpResponse response = restClient.execute(request);

    HttpEntity responseEntity = response.getEntity();

    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        //System.out.println("printing the line in getlastSequenceFromSG" + line);
        if (line.contains("update_seq")) {
          //str1 = line.substring(14);
          String[] arrOfstr = line.split("committed_update_seq");
          String[] arrOfstr1 = arrOfstr[1].split(":");
          String[] arrOfstr2 = arrOfstr1[1].split(",");
          lastsequence = arrOfstr2[0];
          //System.out.println("lastsequence found" + lastsequence);
        }
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          System.err.println(" -= waitForLastSequenceInChangeFeed -= TIMEOUT! for ");
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();

    return lastsequence;
  }

  private void incrementLocalSequenceGlobal(){
    memcachedClient.incr("_sequenceCounterGlobal", 1);
  }

  private String getLocalSequenceForUser(String userName) {
    Object localSegObj = memcachedClient.get("_sequenceCounter_" + userName);
    if (localSegObj == null) {
      syncronizeSequencesForUser(currentIterationUser);
      return memcachedClient.get("_sequenceCounter_" + userName).toString();
    }
    return localSegObj.toString();
  }

  private void incrementLocalSequenceForUser(){
    memcachedClient.incr("_sequenceCounter_" + currentIterationUser , 1);
  }


  private CloseableHttpClient createRestClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
    return clientBuilder.setConnectionManagerShared(true).build();
  }

  private net.spy.memcached.MemcachedClient createMemcachedClient(String memHost, int memPort)
      throws Exception {
    String address = memHost + ":" + memPort;

    return new net.spy.memcached.MemcachedClient(
        new net.spy.memcached.ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),
        net.spy.memcached.AddrUtil.getAddresses(address));
  }

  private void storeRevisions(String responseWithRevision, String userName) {
    //System.err.println("responseWithRevision for validation :" + responseWithRevision);
    Pattern pattern = Pattern.compile("\\\"id\\\".\\\"([^\\\"]*).*\\\"rev\\\".\\\"([^\\\"]*)");
    Matcher matcher = pattern.matcher(responseWithRevision);
    while (matcher.find()) {

      memcachedClient.set(matcher.group(1), 0, matcher.group(2));

    }
  }


  private String readSessionCookie(String responseWithSession) {
    Pattern pattern = Pattern.compile("\\\"session_id\\\".\\\"([^\\\"]*)");
    Matcher matcher = pattern.matcher(responseWithSession);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  private String readRemoteSequence(String responseWithSeq) {
    Pattern pattern = Pattern.compile("\\\"committed_update_seq\\\".([^,]*)");
    Matcher matcher = pattern.matcher(responseWithSeq);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  private boolean lookForDocID(String responseWithDocId, String docId) {
    responseWithDocId = responseWithDocId.replace("},{", "}\n{");
    Pattern pattern = Pattern.compile("\\\"id\\\".\\\"([^\\\"]*)");
    Matcher matcher = pattern.matcher(responseWithDocId);
    boolean found = false;
    while (matcher.find()) {
      if (matcher.group(1).equals(docId)) {
        found = true;
      }
    }
    return found;
  }

  private String getRevision(String key){
    Object respose = memcachedClient.get(key);
    if (respose != null) {
      return memcachedClient.get(key).toString();
    }
    return null;
  }

  private String buildAutorizationBody(String name) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    root.put("name", name);
    root.put("password", DEFAULT_USER_PASSWORD);
    return root.toString();
  }

  private void initAllUsers() {
    int userId = 0;
    while (userId < (totalUsers + insertUsersStart)) {
      userId = sgUsersPool.nextValue() + insertUsersStart;
      if (userId < (totalUsers + insertUsersStart)) {
        String userName = DEFAULT_USERNAME_PREFIX + userId;
        Object storedSession = memcachedClient.get(userName);
        if (storedSession == null) {
          String requestBody = buildAutorizationBody(userName);
          String sessionCookie = null;
          try {
            sessionCookie = httpAuthWithSessionCookie(requestBody);
            memcachedClient.set(userName, 0, sessionCookie);
            syncLocalSequenceWithSyncGatewayForUserAndGlobally(userName);
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

  private void grantAccessToAllUsers(){
    int userId = 0;
    assignRandomUserToCurrentIteration();
    while (userId < (totalUsers + insertUsersStart)) {
      userId = sgAccessPool.nextValue() + insertUsersStart;
      if (userId < (totalUsers + insertUsersStart)) {
        String userName = DEFAULT_USERNAME_PREFIX + userId;
        insertAccessGrant(userName);
      }
    }
  }


  private void syncLocalSequenceWithSyncGatewayForUserAndGlobally(String userName) throws IOException{
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    HttpGet request = new HttpGet("http://" + getRandomHost() + ":" + portAdmin + documentEndpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = restClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();

      String line = "";
      while ((line = reader.readLine()) != null) {
        String remoteSeq = readRemoteSequence(line);
        if (remoteSeq == null) {
          throw new IOException();
        }
        setLocalSequenceForUser(userName, remoteSeq);
        setLocalSequenceGlobally(remoteSeq);
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          restClient.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();
  }


  private void syncronizeSequencesForUser(String userName){
    try {
      syncLocalSequenceWithSyncGatewayForUserAndGlobally(userName);
    } catch (Exception e) {
      System.err.println("Failed to synchronize sequences" + e.getMessage());
    }

  }

  private void assignRandomUserToCurrentIteration() {
    currentIterationUser = DEFAULT_USERNAME_PREFIX + rand.nextInt(totalUsers);
  }

  private String getSessionCookieByUser(String userName) throws IOException {
    String sessionCookie =  memcachedClient.get(currentIterationUser).toString();
    if (sessionCookie == null) {
      throw new IOException("No session cookie stored for user + " + userName);
    }
    return sessionCookie;
  }

  private boolean validateHttpResponse(String response){
    Pattern pattern = Pattern.compile("error");
    Matcher matcher = pattern.matcher(response);
    if (matcher.find()) {
      return false;
    }
    return true;
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
