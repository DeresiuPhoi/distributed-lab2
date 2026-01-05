#!/usr/bin/env python3
"""
Client for testing
Lab 2 - Logical Clocks and Replication Consistency
"""

import argparse
import json
import urllib.request
import urllib.error
import sys


def put_value(node_url, key, value):
    """Send PUT request to node"""
    data = {'key': key, 'value': value}
    
    try:
        req = urllib.request.Request(
            f"{node_url}/put",
            data=json.dumps(data).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        
        with urllib.request.urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"PUT successful:")
            print(json.dumps(result, indent=2))
            return result
            
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        sys.exit(1)


def get_value(node_url, key):
    """Send GET request to node"""
    try:
        with urllib.request.urlopen(f"{node_url}/get?key={key}", timeout=5) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"GET result:")
            print(json.dumps(result, indent=2))
            return result
            
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        sys.exit(1)


def get_status(node_url):
    """Get node status"""
    try:
        with urllib.request.urlopen(f"{node_url}/status", timeout=5) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"Node status:")
            print(json.dumps(result, indent=2))
            return result
            
    except urllib.error.URLError as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='KV Store Client')
    parser.add_argument('--node', required=True, help='Node URL (e.g., http://IP:8000)')
    parser.add_argument('command', choices=['put', 'get', 'status'], help='Command to execute')
    parser.add_argument('args', nargs='*', help='Command arguments')
    
    args = parser.parse_args()
    
    if args.command == 'put':
        if len(args.args) < 2:
            print("Usage: client.py --node URL put KEY VALUE")
            sys.exit(1)
        put_value(args.node, args.args[0], args.args[1])
        
    elif args.command == 'get':
        if len(args.args) < 1:
            print("Usage: client.py --node URL get KEY")
            sys.exit(1)
        get_value(args.node, args.args[0])
        
    elif args.command == 'status':
        get_status(args.node)


if __name__ == '__main__':
    main()