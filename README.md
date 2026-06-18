# MyDFS

MyDFS is a distributed file storage system built from scratch in Go.

The goal of this project is to understand and implement core distributed systems concepts behind large-scale file systems such as chunked storage, content hashing, data integrity verification, networking protocols, concurrency, and fault-tolerant storage architecture.

> **Status:** In Progress 🚧

---

## Overview

MyDFS allows clients to upload files to a distributed storage cluster by splitting files into chunks, hashing each chunk, and distributing them across storage nodes.

The system is being built without external distributed storage frameworks to better understand low-level systems programming and distributed architecture design.

Current architecture focuses on:

- File chunking
- Content hashing
- Checksum verification
- Concurrent chunk uploads using worker pools
- TCP connection pooling
- Custom binary protocol for client-server communication

---

## Features

### Implemented

- **File Chunking**
  - Splits large files into fixed-size chunks (currently 64MB)

- **Content Hashing**
  - Generates deterministic chunk IDs based on content hashes

- **Checksum Verification**
  - Verifies chunk integrity during transfer/storage

- **Concurrent Upload Pipeline**
  - Uploads chunks concurrently using Go worker pools
 
- **Concurrent Download Pipeline**
  - Downloads chunks concurrently using Go worker pools

- **TCP Connection Pool**
  - Reuses TCP connections for efficient communication with chunk servers

- **Custom Protocol**
  - Binary protocol for chunk transfer and metadata exchange
 
---

## Work In Progress

- Metadata server / coordinator
- Chunk replication across multiple nodes

---

## Planned Features

- Fault tolerance and node recovery
- Dockerized deployment
- Kubernetes orchestration
- Health checks and node monitoring
- Distributed consensus / leader election (future)

---

## Tech Stack

- **Language:** Go
- **Networking:** TCP sockets
- **Concurrency:** Goroutines, channels, worker pools
- **Infrastructure (planned):**
  - Docker
  - Kubernetes

---
