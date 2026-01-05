#!/usr/bin/env python3
"""
Distributed Key-Value Store Node with Lamport Clock
Lab 2 - Logical Clocks and Replication Consistency
"""

import json
import argparse
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error

class LamportClock:
    """Lamport logical clock implementation"""
    def __init__(self):
        self.time = 0
        self.lock = threading.Lock()
    
    def increment(self):
        """Increment clock for local event"""
        with self.lock:
            self.time += 1
            return self.time
    
    def update(self, received_time):
        """Update clock on message receipt"""
        with self.lock:
            self.time = max(self.time, received_time) + 1
            return self.time
    
    def get_time(self):
        """Get current clock value"""
        with self.lock:
            return self.time


class KeyValueStore:
    """Replicated key-value store with timestamps"""
    def __init__(self):
        self.store = {}  # {key: {'value': val, 'timestamp': ts, 'node': node_id}}
        self.lock = threading.Lock()
    
    def put(self, key, value, timestamp, node_id):
        """
        Store key-value with timestamp (Last-Writer-Wins)
        Returns True if update was applied, False if rejected
        """
        with self.lock:
            if key not in self.store or timestamp > self.store[key]['timestamp']:
                self.store[key] = {
                    'value': value,
                    'timestamp': timestamp,
                    'node': node_id
                }
                return True
            return False
    
    def get(self, key):
        """Retrieve value for key"""
        with self.lock:
            return self.store.get(key)
    
    def get_all(self):
        """Get all stored data"""
        with self.lock:
            return dict(self.store)


class Node:
    """Distributed node with Lamport clock and KV store"""
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of peer URLs
        self.clock = LamportClock()
        self.store = KeyValueStore()
        self.delay_peer = None  # For Scenario A: artificial delay
        self.delay_seconds = 0
        
    def handle_put(self, key, value):
        """Handle PUT request"""
        # Increment Lamport clock for local event
        timestamp = self.clock.increment()
        
        # Update local store
        self.store.put(key, value, timestamp, self.node_id)
        
        print(f"[{self.node_id}] PUT {key}={value} at Lamport time {timestamp}")
        
        # Replicate to all peers
        self._replicate_to_peers(key, value, timestamp)
        
        return {'status': 'ok', 'timestamp': timestamp, 'node': self.node_id}
    
    def handle_get(self, key):
        """Handle GET request"""
        data = self.store.get(key)
        if data:
            return {
                'status': 'ok',
                'key': key,
                'value': data['value'],
                'timestamp': data['timestamp'],
                'node': data['node']
            }
        else:
            return {'status': 'not_found', 'key': key}
    
    def handle_replicate(self, key, value, timestamp, source_node):
        """Handle replication request from peer"""
        # Update Lamport clock with received timestamp
        local_time = self.clock.update(timestamp)
        
        # Apply update using LWW
        applied = self.store.put(key, value, timestamp, source_node)
        
        if applied:
            print(f"[{self.node_id}] REPLICATE {key}={value} from {source_node} "
                  f"(ts={timestamp}, local_clock={local_time})")
        else:
            print(f"[{self.node_id}] REJECTED {key}={value} from {source_node} "
                  f"(ts={timestamp}, existing timestamp is higher)")
        
        return {'status': 'ok', 'applied': applied}
    
    def handle_status(self):
        """Return node status"""
        all_data = self.store.get_all()
        return {
            'node_id': self.node_id,
            'lamport_time': self.clock.get_time(),
            'store': all_data,
            'peers': self.peers
        }
    
    def _replicate_to_peers(self, key, value, timestamp):
        """Replicate update to all peer nodes"""
        for peer_url in self.peers:
            # Scenario A: Add artificial delay for specific peer
            delay = 0
            if self.delay_peer and peer_url == self.delay_peer:
                delay = self.delay_seconds
            
            # Run replication in separate thread
            thread = threading.Thread(
                target=self._replicate_to_peer,
                args=(peer_url, key, value, timestamp, delay)
            )
            thread.daemon = True
            thread.start()
    
    def _replicate_to_peer(self, peer_url, key, value, timestamp, delay=0):
        """Replicate to a single peer with retry logic"""
        if delay > 0:
            print(f"[{self.node_id}] Delaying replication to {peer_url} by {delay}s")
            time.sleep(delay)
        
        data = {
            'key': key,
            'value': value,
            'timestamp': timestamp,
            'source_node': self.node_id
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                req = urllib.request.Request(
                    f"{peer_url}/replicate",
                    data=json.dumps(data).encode('utf-8'),
                    headers={'Content-Type': 'application/json'}
                )
                
                with urllib.request.urlopen(req, timeout=5) as response:
                    result = json.loads(response.read().decode('utf-8'))
                    print(f"[{self.node_id}] Replicated to {peer_url}: {result}")
                    return
                    
            except Exception as e:
                print(f"[{self.node_id}] Replication to {peer_url} failed (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        print(f"[{self.node_id}] Failed to replicate to {peer_url} after {max_retries} attempts")


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for node API"""
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        params = parse_qs(parsed_path.query)
        
        if path == '/get':
            key = params.get('key', [None])[0]
            if not key:
                self._send_response(400, {'error': 'Missing key parameter'})
                return
            
            result = self.server.node.handle_get(key)
            self._send_response(200, result)
            
        elif path == '/status':
            result = self.server.node.handle_status()
            self._send_response(200, result)
            
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_response(400, {'error': 'Invalid JSON'})
            return
        
        if path == '/put':
            key = data.get('key')
            value = data.get('value')
            
            if not key or value is None:
                self._send_response(400, {'error': 'Missing key or value'})
                return
            
            result = self.server.node.handle_put(key, value)
            self._send_response(200, result)
            
        elif path == '/replicate':
            key = data.get('key')
            value = data.get('value')
            timestamp = data.get('timestamp')
            source_node = data.get('source_node')
            
            if not all([key, value is not None, timestamp, source_node]):
                self._send_response(400, {'error': 'Missing required fields'})
                return
            
            result = self.server.node.handle_replicate(key, value, timestamp, source_node)
            self._send_response(200, result)
            
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def _send_response(self, status_code, data):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass


def main():
    parser = argparse.ArgumentParser(description='Distributed KV Store Node')
    parser.add_argument('--id', required=True, help='Node ID (A, B, or C)')
    parser.add_argument('--port', type=int, required=True, help='Port number')
    parser.add_argument('--peers', required=True, help='Comma-separated peer URLs')
    parser.add_argument('--delay-peer', help='Peer URL to artificially delay (Scenario A)')
    parser.add_argument('--delay-seconds', type=int, default=5, help='Delay in seconds')
    
    args = parser.parse_args()
    
    # Parse peer list
    peers = [p.strip() for p in args.peers.split(',') if p.strip()]
    
    # Create node
    node = Node(args.id, args.port, peers)
    
    # Set delay configuration for Scenario A
    if args.delay_peer:
        node.delay_peer = args.delay_peer
        node.delay_seconds = args.delay_seconds
        print(f"[{args.id}] Artificial delay configured: {args.delay_peer} ({args.delay_seconds}s)")
    
    # Create HTTP server
    server = HTTPServer(('0.0.0.0', args.port), RequestHandler)
    server.node = node
    
    print(f"[{args.id}] Node started on port {args.port}")
    print(f"[{args.id}] Peers: {peers}")
    print(f"[{args.id}] Ready to accept requests...")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{args.id}] Shutting down...")
        server.shutdown()


if __name__ == '__main__':
    main()