# mini-redis extension

This project implements several extension features to the
very good [Mini redis](https://github.com/tokio-rs/mini-redis) project.

## Next steps

- Add command SADD, SREM, SISMEMBER, SLENGTH (unoffical command)

- Unsure:
  - Make a comparison with sqlite latency ?
  - LRU expiration policy or.. when memory max is reached ?

## Explanation on redis protocol

Terminal 1 (without using the cli):
```
  printf '*3\r\n+set\r\n+foo\r\n+bar\r\n' | nc localhost 6379
```
Terminal 2 (using the cli):
```
cargo run --bin mini-redis-cli get foo
"bar"
```
## Demo - A simple game

- Show number of active connections
- Show ranking of players

#### Architecture

React (frontend)
        ↓ HTTP / WebSocket (still unsure)
Backend (Rust service)
        ↓ TCP (RESP) -> Use client wrapper
This Redis implementation

## Supported commands

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [DEL](https://redis.io/commands/del)
* [ZADD](https://redis.io/commands/zadd)
* [ZRANGE](https://redis.io/commands/zrange)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

The Redis wire protocol specification can be found
[here](https://redis.io/topics/protocol).

There is no support for persistence yet.

## License

This project is licensed under the [MIT license](LICENSE).
