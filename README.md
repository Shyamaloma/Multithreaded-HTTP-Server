# Multithreaded HTTP/1.1 Server (C)

## Overview

This project is a **multithreaded HTTP/1.1 server implemented in C** that supports concurrent client requests using a thread pool and a synchronized work queue. The server handles basic HTTP methods (`GET` and `PUT`) while ensuring **thread-safe file access** through fine-grained reader-writer locks.

The system is designed to demonstrate core systems programming concepts including:

* Low-level socket programming
* Multithreading with POSIX threads
* Synchronization (mutexes and reader-writer locks)
* HTTP protocol parsing
* File I/O and error handling

---

## Features

* **Concurrent Request Handling**

  * Thread pool architecture with a shared request queue
  * Multiple clients can interact with the server simultaneously

* **Supported HTTP Methods**

  * `GET`: Retrieve files from the server
  * `PUT`: Upload or overwrite files on the server

* **Thread-Safe File Access**

  * Per-URI reader-writer locks
  * Multiple readers allowed, exclusive writers enforced

* **Robust Request Parsing**

  * Regex-based validation of HTTP requests
  * Handles malformed requests with proper HTTP error responses

* **Logging**

  * Logs all requests to `stderr` in the format:

    ```
    METHOD,/uri,status_code,request_id
    ```

---

## Architecture

### Thread Model

* **Dispatcher Thread (Main Thread)**

  * Accepts incoming client connections
  * Pushes connection file descriptors into a shared queue

* **Worker Threads**

  * Continuously pull requests from the queue
  * Process HTTP requests independently

### Synchronization

* **Queue**

  * Thread-safe producer-consumer queue for connection handling

* **Per-URI Locks**

  * Each file (URI) is associated with a reader-writer lock
  * Prevents race conditions during concurrent access

---

## Build Instructions

Compile the server using the provided Makefile:

```
make
```

This will generate the executable (not committed to the repo).

---

## Usage

Run the server on a specified port:

```
./httpserver [-t threads] <port>
```

### Arguments

* `-t <threads>`: Number of worker threads (default: 4)
* `<port>`: Port number (1–65535)

---

## Example Requests

### GET Request

```
curl http://localhost:8080/file.txt
```

### PUT Request

```
curl -X PUT -d "Hello World" http://localhost:8080/file.txt
```

---

## HTTP Responses

| Status Code               | Description                     |
| ------------------------- | ------------------------------- |
| 200 OK                    | Successful GET or overwrite PUT |
| 201 Created               | File created via PUT            |
| 400 Bad Request           | Malformed request               |
| 403 Forbidden             | Invalid file access             |
| 404 Not Found             | File does not exist             |
| 500 Internal Server Error | Server-side failure             |
| 501 Not Implemented       | Unsupported method              |
| 505 Version Not Supported | Invalid HTTP version            |

---

## Key Implementation Details

### Request Parsing

* Uses **regular expressions** to validate request format:

  ```
  METHOD URI HTTP/1.1
  ```
* Rejects malformed or unsupported requests early

### GET Handling

* Acquires **reader lock**
* Reads file in chunks and streams to client
* Ensures safe concurrent reads

### PUT Handling

* Acquires **writer lock**
* Validates `Content-Length`
* Writes request body to file safely
* Handles partial reads and large payloads

### Concurrency Design

* Thread pool reduces overhead of thread creation
* Queue ensures efficient load distribution
* Fine-grained locking avoids global bottlenecks

---

## What This Project Demonstrates

* Systems-level programming in C
* Understanding of HTTP protocol basics
* Safe concurrency and synchronization
* Efficient I/O handling
* Clean modular design

---

## Future Improvements

* Support for additional HTTP methods (POST, DELETE)
* Persistent connections (keep-alive)
* Improved request parsing without regex
* Performance benchmarking and optimization

---

## Author

Shyam Kishan
