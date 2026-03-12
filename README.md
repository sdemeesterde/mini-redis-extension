# mini-redis extension

This project implements several extension features to the
very good [Mini redis](https://github.com/tokio-rs/mini-redis) project.

## Next steps

- Implement AOF (append to end of file)
- Offer 2 AOF strategy:
  - write + fsync every command
  - write + fsync every command
- ? Expiration or a certain policy when memory max is reached ?
Like LRU..

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

## Supported commands

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [DEL](https://redis.io/commands/del)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

The Redis wire protocol specification can be found
[here](https://redis.io/topics/protocol).

There is no support for persistence yet.

## License

This project is licensed under the [MIT license](LICENSE).
