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
import org.apache.http.HttpEntity;
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
  private static final String HTTP_CON_TIMEOUT = "rest.timeout.con";
  private static final String HTTP_READ_TIMEOUT = "rest.timeout.read";
  private static final String HTTP_EXEC_TIMEOUT = "rest.timeout.exec";
  private static final String HTTP_HEADERS = "headers";

  private static final String SG_HOST = "syncgateway.host";
  private static final String SG_DB = "syncgateway.db";
  private static final String SG_PORT_ADMIN = "syncgateway.port.admin";
  private static final String SG_PORT_PUBLIC = "syncgateway.port.public";
  private static final String SG_AUTH = "syncgateway.auth";
  private static final String SG_LOAD_MODE = "syncgateway.loadmode";
  private static final String SG_READ_MODE = "syncgateway.readmode";
  private static final String SG_INSERT_MODE = "syncgateway.insertmode";
  private static final String SG_BULK_SIZE = "syncgateway.bulksize";
  private static final String SG_ROUD_TRIP_WRITE = "syncgateway.roundtrip";
  private static final String SG_TOTAL_USERS = "syncgateway.totalusers";
  private static final String SG_TOTAL_CHANNELS = "syncgateway.channels";
  private static final String SG_SEQUENCE_START = "syncgateway.sequencestart";
  private static final String SG_INSERTUSERS_START = "syncgateway.insertusersstart";

  private static final String SG_INIT_USERS = "syncgateway.initusers";

  private static final String MEMCACHED_HOST = "memcached.host";
  private static final String MEMCACHED_PORT = "memcached.port";

  private static final String DEFAULT_USERNAME_PREFIX = "sg-user-";
  private static final String DEFAULT_CHANNEL_PREFIX = "channel-";
  private static final String DEFAULT_USER_PASSWORD = "password";

  private static final int SG_LOAD_MODE_USERS = 0;
  private static final int SG_LOAD_MODE_DOCUMENTS  = 1;

  private static final int SG_INSERT_MODE_BYKEY= 0;
  private static final int SG_INSERT_MODE_BYUSER  = 1;

  private static final int SG_RUN_MODE_BULK = 0;
  private static final int SG_RUN_MODE_SINGLE  = 1;
  private static final int SG_RUN_MODE_CHANGESONLY  = 2;


  // Sync Gateway parameters
  private String portAdmin;
  private String portPublic;
  private boolean useAuth;
  private int bulkSize = 10;
  private boolean roudTripWrite;
  private int loadMode;
  private int readMode;
  private int totalUsers;
  private int totalChannels;
  private String[] hosts;
  private String host;
  private int insertMode;
  private String sequencestart;
  private boolean initUsers;
  private int insertUsersStart=0;


  // http parameters
  private volatile Criteria requestTimedout = new Criteria(false);
  private String[] headers;
  private int conTimeout = 10000;
  private int readTimeout = 10000;
  private int execTimeout = 1000;
  private CloseableHttpClient restClient;


  //memcached
  private MemcachedClient memcachedClient;

  //private String urlEndpointPrefix;
  private String createUserEndpoint;
  private String documentEndpoint;
  private String createSessionEndpoint;
  private String bulkPostEndpoint;
  private String bulkGetEndpoint;

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
    loadMode = (props.getProperty(SG_LOAD_MODE, "documents").equals("users")) ?
        SG_LOAD_MODE_USERS : SG_LOAD_MODE_DOCUMENTS;

    insertMode = (props.getProperty(SG_INSERT_MODE, "bykey").equals("bykey")) ?
        SG_INSERT_MODE_BYKEY : SG_INSERT_MODE_BYUSER;

    String runModeProp = props.getProperty(SG_READ_MODE, "single");
    if (runModeProp.equals("single")) {
      readMode = SG_RUN_MODE_SINGLE;
    } else if (runModeProp.equals("bulk")) {
      readMode = SG_RUN_MODE_BULK;
    } else {
      readMode = SG_RUN_MODE_CHANGESONLY;
    }

    totalUsers = Integer.valueOf(props.getProperty(SG_TOTAL_USERS, "1000"));
    totalChannels = Integer.valueOf(props.getProperty(SG_TOTAL_CHANNELS, "100"));
    bulkSize = Integer.valueOf(props.getProperty(SG_BULK_SIZE, "100"));
    roudTripWrite = props.getProperty(SG_ROUD_TRIP_WRITE, "false").equals("true");
    initUsers = props.getProperty(SG_INIT_USERS, "true").equals("true");

    insertUsersStart = Integer.valueOf(props.getProperty(SG_INSERTUSERS_START, "0"));
    conTimeout = Integer.valueOf(props.getProperty(HTTP_CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(HTTP_READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(HTTP_EXEC_TIMEOUT, "10")) * 1000;
    headers = props.getProperty(HTTP_HEADERS, "Accept */* Content-Type application/json user-agent Mozilla/5.0 ").
        trim().split(" ");

    String memcachedHost = props.getProperty(MEMCACHED_HOST, "localhost");
    String memcachedPort = props.getProperty(MEMCACHED_PORT, "8000");

    sequencestart = props.getProperty(SG_SEQUENCE_START, "2000001");

    createUserEndpoint = "/" + db + "/_user/";
    documentEndpoint =  "/" + db + "/";
    createSessionEndpoint = "/" + db + "/_session";
    bulkPostEndpoint = documentEndpoint + "_bulk_docs";
    bulkGetEndpoint = documentEndpoint + "_bulk_get";

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

  }


  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

    assignRandomUserToCurrentIteration();

    if (readMode == SG_RUN_MODE_BULK) {
      return readBulk(table, key, fields, result);
    } else if (readMode == SG_RUN_MODE_SINGLE) {
      return readSingle(table, key, fields, result);
    }
    return readChanges(key);
  }

  private Status readChanges(String key) {
    try {
      String seq = getLocalSequence(currentIterationUser);
      checkForChanges(seq, getChannelNameByKey(key));
    } catch (Exception e) {
      return Status.ERROR;
    }
    syncronizeSequences(currentIterationUser);

    return Status.OK;
  }


  private Status readSingle(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    String port = (useAuth) ? portPublic : portAdmin;
    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + key;

    int responseCode;
    try {
      responseCode = httpGet(fullUrl, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "GET");
    }

    return getStatus(responseCode);
  }

  private Status readBulk(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

    String port = (useAuth) ? portPublic : portAdmin;
    String requestBody = buildDocumentFromReadBulk(key);
    String fullUrl = "http://" + getRandomHost() + ":" + port + bulkGetEndpoint;
    HttpPost httpPostRequest = new HttpPost(fullUrl);

    int responseCode;
    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }
    return getStatus(responseCode);

  }


  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    assignRandomUserToCurrentIteration();
    if ((readMode == SG_RUN_MODE_BULK)) {
      return Status.NOT_IMPLEMENTED;
    }

    String requestBody = buildDocumentFromMap(key, values);
    String docRevision = getRevision(key);

    if (docRevision == null) {
      System.err.println("Revision for document " + key + " not found in local");
      return Status.UNEXPECTED_STATE;
    }

    String currentSequence = getLocalSequence(currentIterationUser);
    String port = (useAuth) ? portPublic : portAdmin;
    String fullUrl;
    int responseCode;

    fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + key + "?rev=" + docRevision;
    HttpPut httpPutRequest = new HttpPut(fullUrl);

    try {
      responseCode = httpExecute(httpPutRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "PUT");
    }

    Status result = getStatus(responseCode);
    if (result == Status.OK) {
      incrementLocalSequence();
      if (roudTripWrite) {
        if ((currentSequence == null) || (currentSequence.equals(""))) {
          System.err.println("Memcached failure!");
          return Status.BAD_REQUEST;
        }
        try {
          waitForDocInChangeFeed(currentSequence, key);
        } catch (Exception e) {
          syncronizeSequences(currentIterationUser);
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
      incrementLocalSequence();
    }

    return result;
  }


  private Status insertDocument(String table, String key, HashMap<String, ByteIterator> values) {


    if (readMode == SG_RUN_MODE_BULK && roudTripWrite) {
      return Status.NOT_IMPLEMENTED;
    }

    String port = (useAuth) ? portPublic : portAdmin;
    String requestBody;
    String fullUrl;

    if (readMode == SG_RUN_MODE_BULK) {
      requestBody = buildDocumentFromMapBulk(key, values);
      fullUrl = "http://" + getRandomHost() + ":" + port + bulkPostEndpoint;
    } else {
      requestBody = buildDocumentFromMap(key, values);
      fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint;
    }

    String currentSequence = getLocalSequence(currentIterationUser);
    HttpPost httpPostRequest = new HttpPost(fullUrl);
    int responseCode;
    //System.out.println("before INSERT: User " + currentIterationUser + " just inserted doc " + key
    //    + " SG seq before insert was " + currentSequence);

    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }

    //System.out.println("INSERT: User " + currentIterationUser + " just inserted doc " + key
    //    + " SG seq before insert was " + currentSequence);

    Status result = getStatus(responseCode);
    if (result == Status.OK) {
      incrementLocalSequence();
      if (roudTripWrite) {
        if ((currentSequence == null) || (currentSequence.equals(""))) {
          System.err.println("Memcached failure!");
          return Status.BAD_REQUEST;
        }
        try {
          waitForDocInChangeFeed(currentSequence, key);
        } catch (Exception e) {
          syncronizeSequences(currentIterationUser);
          return Status.UNEXPECTED_STATE;
        }
      }
    }

    return result;
  }


  private void checkForChanges(String sequenceSince, String channel) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=normal&include_docs=true";
    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

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

    if (true) {
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
    }
    restClient.close();
  }


  private void waitForDocInChangeFeed(String sequenceSince, String key) throws IOException {
    String port = (useAuth) ? portPublic : portAdmin;
    /*
    String changesFeedEndpoint = "_changes?since=" + sequenceSince +
        "&feed=longpoll&filter=sync_gateway/bychannel&channels=" + getChannelNameByKey(key);
        */
    String changesFeedEndpoint = "_changes?since=" + sequenceSince + "&feed=longpoll";
    String fullUrl = "http://" + getRandomHost() + ":" + port + documentEndpoint + changesFeedEndpoint;

    //System.out.println("After Insert, checking for feed:" + fullUrl);


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

    boolean docFound = false;
    while (!docFound) {
      CloseableHttpResponse response = restClient.execute(request);
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        InputStream stream = responseEntity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuffer responseContent = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
          if (lookForDocID(line, key)) {
            docFound = true;
          }
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

    if (useAuth) {
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
    CloseableHttpResponse response = restClient.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    boolean responseGenericValidation = true;
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        storeRevisions(line, currentIterationUser);
        responseGenericValidation = validateHttpResponse(line);
        if (requestTimedout.isSatisfied()) {
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
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    if (useAuth) {
      request.setHeader("Cookie", "SyncGatewaySession=" + getSessionCookieByUser(currentIterationUser));
    }

    CloseableHttpResponse response = restClient.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
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
      while ((line = reader.readLine()) != null)  {
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

  private String getRandomHost(){
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

    int channelsPerUser = 100;
    int step = totalUsers / 100;
    int limit1 = step * 50;
    int limit2 = limit1 + (step*20);
    if (id > limit1) {
      if (id > limit2) {
        channelsPerUser = 20;
      } else {
        channelsPerUser = 50;
      }
    }

    String[] channelsSet = getSetOfRandomChannels(channelsPerUser);
    for (int i=0; i<channelsPerUser; i++){
      channels.add(channelsSet[i]);
    }
    saveChannelForUser(userName, channelsSet[0]);
    //channels.add("*");
    root.set("admin_channels", channels);
    return root.toString();
  }

  private String buildDocumentFromMap(String key, HashMap<String, ByteIterator> values) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    ArrayNode channelsNode = factory.arrayNode();
    root.put("_id", key);

    channelsNode.add("ycsb");

    if (insertMode == SG_INSERT_MODE_BYKEY) {
      channelsNode.add(getChannelNameByKey(key));
    } else {
      channelsNode.add(getChannelForUser());
    }

    root.set("channels", channelsNode);

    values.forEach((k, v)-> {
        root.put(k, v.toString());
      });
    return root.toString();
  }

  private String buildDocumentFromMapBulk(String key, HashMap<String, ByteIterator> values) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    ArrayNode docsNode = factory.arrayNode();
    ArrayNode channelsNode;
    ObjectNode docStaticFields = factory.objectNode();

    values.forEach((k, v)-> {
        docStaticFields.put(k, v.toString());
      });

    for (int i = 0; i<bulkSize; i++) {
      ObjectNode docInstance = docStaticFields.deepCopy();
      docInstance.put("_id", key + "_bulk" + i);
      channelsNode = factory.arrayNode();
      channelsNode.add("ycsb");
      channelsNode.add(getChannelNameByKey(key));
      docInstance.set("channels", channelsNode);
      docsNode.add(docInstance);
    }

    root.set("docs", docsNode);
    root.put("new_edits", true);

    return root.toString();
  }

  private String getChannelNameByKey(String key){
    int channelId = Math.abs(key.hashCode() % totalChannels);
    return DEFAULT_CHANNEL_PREFIX + channelId;
  }

  private String[] getSetOfRandomChannels(int channelsPerUser) {
    if (channelsPerUser > totalChannels) {
      channelsPerUser = totalChannels;
    }
    String[] channels = new String[channelsPerUser];
    int[] allChannels = new int[totalChannels];

    for (int i=0; i<totalChannels; i++){
      allChannels[i] = i;
    }
    shuffleArray(allChannels);
    for (int i=0; i<channelsPerUser; i++){
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

  private void setLocalSequence(String userName, String seq) {
    memcachedClient.set("_sequenceCounter_" + userName, 0, seq);
  }

  private String getLocalSequence(String userName) {
    Object localSegObj = memcachedClient.get("_sequenceCounter_" + userName);
    if (localSegObj == null) {
      syncronizeSequences(currentIterationUser);
      return memcachedClient.get("_sequenceCounter_" + userName).toString();
    }
    return localSegObj.toString();
  }

  private void incrementLocalSequence(){
    memcachedClient.incr("_sequenceCounter_" + currentIterationUser , 1);
  }

  private String buildDocumentFromReadBulk(String key) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    ArrayNode docsNode = factory.arrayNode();

    for (int i = 0; i<bulkSize; i++) {
      docsNode.add(factory.objectNode().put("id", key + "_bulk" + i));
    }
    root.set("docs", docsNode);
    return root.toString();
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
    if (readMode == SG_RUN_MODE_BULK) {
      responseWithRevision = responseWithRevision.replace("},{", "}\n{");
    }
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
          } catch (Exception e) {
            System.err.println("Autorization failure for user " + userName + ", exiting...");
            System.exit(1);
          }
        }
        setLocalSequence(userName, sequencestart);
      }
    }
  }


  private void syncLocalSequenceWithSyncGateway(String userName) throws IOException{
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
        setLocalSequence(userName, remoteSeq);
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


  private void syncronizeSequences(String userName){
    try {
      syncLocalSequenceWithSyncGateway(userName);
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
