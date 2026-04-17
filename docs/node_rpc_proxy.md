# Base Node RPC Proxy

This document describes the recommended way to let Dockerized LedgerFlow services reach a Base node that is intentionally bound only to:

`127.0.0.1:8545`

The goal is:

- keep the node private on loopback
- avoid exposing the node directly on a public or LAN interface
- allow only Docker containers on this host to reach it

## Recommended shape

Run nginx on the host and make it:

- listen on a separate host port such as `18545`
- proxy requests to `127.0.0.1:8545`
- allow only the Docker bridge subnet
- deny everything else

LedgerFlow containers then use:

`http://host.docker.internal:18545`

instead of trying to talk to the node directly.

## Why this is safer

Your node keeps listening only on:

`127.0.0.1:8545`

That means the node itself is still not reachable from outside the host.

nginx becomes the only external listener, and nginx can enforce:

- source IP allow lists
- rate limits if you want them later
- access logs

## nginx config

Start from:

`ops/nginx/base-node-rpc-proxy.conf.example`

Example:

```nginx
server {
    listen 18545;
    server_name _;

    allow 172.17.0.0/16;
    deny all;

    location / {
        proxy_pass http://127.0.0.1:8545;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Connection "";
        proxy_connect_timeout 5s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
}
```

If your Docker subnet is not `172.17.0.0/16`, replace that CIDR.

## How to find your Docker subnet

For the default bridge:

```bash
docker network inspect bridge --format '{{(index .IPAM.Config 0).Subnet}}'
```

For a Compose network:

```bash
docker network ls
docker network inspect YOUR_NETWORK_NAME --format '{{(index .IPAM.Config 0).Subnet}}'
```

If you want to see a specific running container's network:

```bash
docker inspect YOUR_CONTAINER_NAME --format '{{json .NetworkSettings.Networks}}'
```

## What to allow

Do not allow one container IP if you can avoid it.

Why:

- container IPs are not stable
- recreating containers often changes them

Prefer allowing the Docker subnet instead.

Examples:

- `172.17.0.0/16`
- `172.18.0.0/16`
- or your Compose project's specific subnet

## How to test the proxy

First test from the host:

```bash
curl -s http://127.0.0.1:18545 \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

Then test from a container view:

```bash
docker run --rm --add-host=host.docker.internal:host-gateway curlimages/curl:8.7.1 \
  -s http://host.docker.internal:18545 \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

Both should return a JSON-RPC response with `result`.

## LedgerFlow configuration

When using the proxy, set:

```env
LEDGERFLOW_RPC_URL=http://host.docker.internal:18545
LEDGERFLOW_INDEXER_RPC_URL=http://host.docker.internal:18545
```

For local non-Docker runs on the host itself, you can use:

```env
LEDGERFLOW_RPC_URL=http://127.0.0.1:18545
LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:18545
```
