# mini-redis extension

This project extends the very good [mini-redis](https://github.com/tokio-rs/mini-redis) project with additional features (commands, AOF, etc.).

---

## Getting started

First, run the server:

```bash
cargo run --bin miniredis-server
```

You can run it either from code or directly via the CLI like above.

---

## Usage

### Client

Basic example:

```rust
use miniredis::clients::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379").await.unwrap();

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}
```

---

### Stream (raw RESP)

With the server running, you can send commands using the Redis [RESP protocol](https://redis.io/docs/latest/develop/reference/protocol-spec):

```bash
printf '*3\r\n+set\r\n+foo\r\n+bar\r\n' | nc localhost 6379
```

---

### Buffered client approach

Using `BufferedClient` avoids issues in async contexts (like interleaved frames):

```rust
use miniredis::clients::{BufferedClient, Client};

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379").await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let pong = buffered_client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}
```

---

### CLI

You can also interact via the CLI:

```bash
cargo run --bin miniredis-cli get foo
"bar"
```

---

## Supported commands

Commands marked with `*` are **newly added in this project**.

### Core

* PING
* GET
* SET
* DEL *

---

### Sets

* SADD *
* SISMEMBER *
* SREM *
* SLENGTH * *(unofficial)*

---

### Sorted sets

* ZADD *
* ZRANGE *
* ZREM *
* ZSCORE *
* ZLENGTH * *(unofficial)*

---

### Pub/Sub

* PUBLISH
* SUBSCRIBE

---

Full Redis protocol spec:
[https://redis.io/topics/protocol](https://redis.io/topics/protocol)

---

## AOF (Append-Only File)

Strategy: message passing.

* A single async task collects frames
* Flushes them in batches every second
* Reduces I/O calls

Simple approach, but good enough for this use case.

---

## Tests

The project includes tests for:

* client
* buffered client
* server (stream / RESP)

---

## License

This project is licensed under the [MIT license](LICENSE).
