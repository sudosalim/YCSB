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


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.generator.CounterGenerator;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;


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
  private static final String CON_TIMEOUT = "rest.timeout.con";
  private static final String READ_TIMEOUT = "rest.timeout.read";
  private static final String EXEC_TIMEOUT = "rest.timeout.exec";
  private static final String HEADERS = "headers";

  private static final String SG_HOST = "syncgateway.host";
  private static final String SG_DB = "syncgateway.db";
  private static final String SG_PORT_ADMIN = "syncgateway.port.admin";
  private static final String SG_PORT_PUBLIC = "syncgateway.port.public";
  private static final String SG_AUTH = "syncgateway.auth";
  private static final String SG_CREATEUSERS = "syncgateway.createusers";

  private static final String USERNAME_PREFIX = "sg-user-";
  private static final String REQUEST_BODY_CREATE_USER =
      "{ \"name\": \"%s\", \"password\": \"password\", " +
          "\"admin_channels\": [\"%s\"], " + "\"all_channels\":[\"%s\"]}";

  // Sync Gateway parameters
  private String host;
  private String db;
  private String portAdmin;
  private String portPublic;
  private boolean useAuth;
  private boolean createUsers;
  private String currentUser = null;

  // http parameters
  private volatile Criteria requestTimedout = new Criteria(false);
  private String[] headers;
  private CloseableHttpClient restClient;
  private int conTimeout = 10000;
  private int readTimeout = 10000;
  private int execTimeout = 10000;

  private String urlEndpointPrefix;
  private String createUserEndPoint;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    host = props.getProperty(SG_HOST, "127.0.0.1");
    db = props.getProperty(SG_DB, "db");
    portAdmin = props.getProperty(SG_PORT_ADMIN, "4985");
    portPublic = props.getProperty(SG_PORT_PUBLIC, "4984");
    useAuth = props.getProperty(SG_AUTH, "false").equals("true");
    createUsers = props.getProperty(SG_CREATEUSERS, "false").equals("true");

    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    headers = props.getProperty(HEADERS, "Accept */* Content-Type application/xml user-agent Mozilla/5.0 ").
        trim().split(" ");

    setupClient();
    if (useAuth) {
      assignUserName();
    }

    urlEndpointPrefix = "http://" + host + ":";
    createUserEndPoint = "/" + db + "/_user/";

  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (createUsers) {
      insertUser(table, key, values);
    } else {
      insertDocument(table, key, values);
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }

  private Status insertUser(String table, String key, HashMap<String, ByteIterator> values) {

    String username = USERNAME_PREFIX + sgUserInsertCounter.nextValue();
    String channelName = username + "-channel";

    //{ "name": "<name>", "password": "password", "admin_channels": ["<channel>"], "all_channels":["<channel>"]}
    String requestBody = String.format(REQUEST_BODY_CREATE_USER, username, channelName, channelName);
    HttpPost httpPostRequest;
    String fullUrl;

    if (useAuth) {
      fullUrl = urlEndpointPrefix + portPublic + createUserEndPoint;
      httpPostRequest = new HttpPost(fullUrl);
    } else {
      fullUrl = urlEndpointPrefix + portAdmin + createUserEndPoint;
      httpPostRequest = new HttpPost(fullUrl);
    }

    int responseCode;
    try {
      responseCode = httpExecute(httpPostRequest, requestBody);
    } catch (Exception e) {
      responseCode = handleExceptions(e, fullUrl, "POST");
    }

    return getStatus(responseCode);
  }

  private Status insertDocument(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }


  private void assignUserName() {
    currentUser =  USERNAME_PREFIX + sgUsersPool.nextValue();
  }


  private int httpExecute(HttpEntityEnclosingRequestBase request, String data) throws IOException {
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
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    restClient.close();
    return responseCode;
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

  private int handleExceptions(Exception e, String url, String method) {
    System.err.println(new StringBuilder(method).append(" Request: ").append(url).append(" | ")
        .append(e.getClass().getName()).append(" occured | Error message: ")
        .append(e.getMessage()).toString());

    if (e instanceof ClientProtocolException) {
      return 400;
    }
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

  private void setupClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
    this.restClient = clientBuilder.setConnectionManagerShared(true).build();
  }

}
