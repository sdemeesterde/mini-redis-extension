# miniredis extension

This project implements several extension features to the
very good [Mini redis](https://github.com/tokio-rs/mini-redis) project.

## Next steps

- Potential next steps:
  - Make a comparison with sqlite latency ?
  - LRU expiration policy or.. when memory max is reached ?

## Explanation on redis protocol

Terminal 1 (without using the cli):
```
  printf '*3\r\n+set\r\n+foo\r\n+bar\r\n' | nc localhost 6379
```
Terminal 2 (using the cli):
```
cargo run --bin miniredis-cli get foo
"bar"
```
## Demo - A simple game

- Show number of active connections
- Show ranking of players

## Supported commands

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [DEL](https://redis.io/commands/del)
* LEN: Unofficial redis command
* [SADD](https://redis.io/docs/latest/commands/sadd)
* [SISMEMBER](https://redis.io/docs/latest/commands/sismember)
* SLENGTH: Unofficial redis command
* [SREM](https://redis.io/docs/latest/commands/srem)
* [ZADD](https://redis.io/commands/zadd)
* [ZRANGE](https://redis.io/commands/zrange)
* [ZRANK](https://redis.io/docs/latest/commands/zrank) With different optional parameters.
* [ZREM](https://redis.io/docs/latest/commands/zrem)
* [ZSCORE](https://redis.io/docs/latest/commands/zscore)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

The Redis wire protocol specification can be found
[here](https://redis.io/topics/protocol).

There is no support for persistence yet.

## License

This project is licensed under the [MIT license](LICENSE).
