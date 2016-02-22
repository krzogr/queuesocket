# queuesocket
Queue based socket implementation for acceptance testing.

# Introduction
Queuesocket is custom implementation of *java.net.Socket* which uses in-memory queues to exchange bytes within single process. 
Queue sockets can be used in acceptance testing to isolate the application which is being tested from external processes and the network.  

# Build
Requirements:
- JRE 1.8
- Maven 3.3.1

```
mvn install
```

# Sample Usage

```java
  @Test
  public void testHttpClient() throws Exception {
    // Set queue socket as the default socket provider in the application
    QueueSocketFactory.setup();

    ExecutorService executor = Executors.newFixedThreadPool(1);
    
    // Fetch website content in background thread and return it
    Future<String> website = executor.submit(() -> {
      HttpClient client = HttpClients.createDefault();
      HttpGet request = new HttpGet("http://www.server.com/FancyWebsite");
      
      HttpResponse response = client.execute(request);
      
      HttpEntity entity = response.getEntity();
      return IOUtils.toString(entity.getContent());
    });

    // Accept socket connection to http://www.server.com
    QueueSocketManager manager = QueueSocketManager.getInstance();
    QueueSocketEndpoint endpoint = manager.accept("www.server.com", 80, 1, SECONDS);

    // Verify that the connection has been established
    assertNotNull(endpoint);

    // Verify the request sent by HttpClient
    String request = endpoint.getNextReceivedString(1, SECONDS);
    assertTrue(request.contains("GET /FancyWebsite HTTP/1.1"));
    assertTrue(request.contains("Host: www.server.com"));
    
    // Generate HTTP response
    endpoint.sendMessage("HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nabcde");
    
    // Verify that the response was received by HttpClient
    String response = website.get(1, SECONDS);
    assertNotNull(response);
    assertEquals("abcde", response);
    
    executor.shutdownNow();
  }
```

