# Lab 2: Distributed Key-Value Store with Lamport Clocks

## Overview

A replicated key-value store implementation running on 3 AWS EC2 nodes with Lamport logical clocks for causal ordering.

## Architecture

- 3 EC2 nodes (A, B, C) running Ubuntu 22.04
- HTTP-based communication (JSON payloads)
- Lamport clock for event ordering
- Last-Writer-Wins conflict resolution

## Setup

```bash
# Start Node A
python3 node.py --id A --port 8000 --peers http://<IP-B>:8001,http://<IP-C>:8002

# Start Node B
python3 node.py --id B --port 8001 --peers http://<IP-A>:8000,http://<IP-C>:8002

# Start Node C
python3 node.py --id C --port 8002 --peers http://<IP-A>:8000,http://<IP-B>:8001
```

## Testing

```bash
# PUT operation
python3 client.py --node http://<IP>:8000 put key value

# GET operation
python3 client.py --node http://<IP>:8000 get key

# Check status
python3 client.py --node http://<IP>:8000 status
```

## Scenarios Tested

1. **Delay/Reorder**: Artificial network delay
2. **Concurrent Writes**: Last-Writer-Wins conflict resolution
3. **Temporary Outage**: Node failure and recovery

## Key Findings

- Lamport clocks successfully maintain causal ordering
- Last-Writer-Wins effectively resolves conflicts
- Basic replication lacks automatic recovery mechanisms

## Author

[Your Name]

## Date

January 2026
