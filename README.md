# mini-redis extension

This project implements several extension features to the
very good [Mini redis](https://github.com/tokio-rs/mini-redis) project.

## Next steps

- Implement
- Implement AOF (append to end of file)
- Offer 2 AOF strategy:
  - write + fsync every command
  - write + fsync every command

## Supported commands

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

The Redis wire protocol specification can be found
[here](https://redis.io/topics/protocol).

There is no support for persistence yet.

## License

This project is licensed under the [MIT license](LICENSE).
