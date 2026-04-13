use miniredis::{clients::Client, server};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A PING PONG test without message provided.
/// It should return "PONG".
#[tokio::test]
async fn ping_pong_without_message() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}

/// A PING PONG test with message provided.
/// It should return the message.
#[tokio::test]
async fn ping_pong_with_message() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(Some("你好世界".into())).await.unwrap();
    assert_eq!("你好世界".as_bytes(), &pong[..]);
}

/// A basic "hello world" style test. A server instance is started in a
/// background task. A client instance is then established and set and get
/// commands are sent to the server. The response is then evaluated
#[tokio::test]
async fn key_value_get_set() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

/// Simple set() & len() call to test len()
#[tokio::test]
async fn set_len() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let len = client.len().await.unwrap();
    assert_eq!(0, len);

    client.set("foo1", "bar1".into()).await.unwrap();
    client.set("foo2", "bar2".into()).await.unwrap();

    let len = client.len().await.unwrap();
    assert_eq!(2, len);
}

/// A delete test with single delete. Try to delete an existing and non
/// existing key
#[tokio::test]
async fn single_delete() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("foo", "bar".into()).await.unwrap();

    let deleted = client.del(vec!["foo".to_string()]).await.unwrap();
    assert_eq!(1, deleted);

    let deleted = client.del(vec!["unknown_key".to_string()]).await.unwrap();
    assert_eq!(0, deleted);
}

/// A delete test for several deletes:
/// - Two existing keys
/// - One existing, one none existing key
/// - Two no existing keys
#[tokio::test]
async fn several_deletes() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("foo1", "bar1".into()).await.unwrap();
    client.set("foo2", "bar2".into()).await.unwrap();

    let members = vec![String::from("foo1"), String::from("foo2")];

    let cnt = client.del(members.clone()).await.unwrap();
    assert_eq!(2, cnt);

    client.set("foo1", "bar1".into()).await.unwrap();

    let cnt = client.del(members.clone()).await.unwrap();
    assert_eq!(1, cnt);

    let cnt = client.del(members).await.unwrap();
    assert_eq!(0, cnt);
}

/// `S`: Add and retrieve length of underlying set.
#[tokio::test]
async fn sadd_slength() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key1";
    let members = vec![String::from("player1")];

    let added = client.sadd(key, members.clone()).await.unwrap();
    assert_eq!(1, added);
    let added = client.sadd(key, members).await.unwrap();
    assert_eq!(0, added);

    let length = client.slength(key).await.unwrap();
    assert_eq!(1, length);

    let members = vec![String::from("player2"), String::from("player3")];
    let added = client.sadd(key, members).await.unwrap();

    assert_eq!(2, added);

    let length = client.slength(key).await.unwrap();
    assert_eq!(3, length);

    let unknown_key = "unknown";
    let length = client.slength(unknown_key).await.unwrap();
    assert_eq!(0, length);
}

/// `S`: Add and ismember of underlying set.
#[tokio::test]
async fn sadd_sismember() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key1";
    let members = vec![String::from("player2"), String::from("player3")];
    client.sadd(key, members).await.unwrap();

    let not_member = client.sismember(key, "player1").await.unwrap();
    assert_eq!(0, not_member);

    let is_member = client.sismember(key, "player2").await.unwrap();
    assert_eq!(1, is_member);
}

/// `S`: Add and rem of underlying set.
#[tokio::test]
async fn sadd_srem() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key1";
    let members = vec![
        String::from("player1"),
        String::from("player2"),
        String::from("player3"),
    ];
    client.sadd(key, members).await.unwrap();

    // Removed nothing
    let members = vec![String::from("player4")];
    let removed = client.srem(key, members).await.unwrap();
    assert_eq!(0, removed);

    // Removed one value
    let members = vec![String::from("player1")];
    let removed = client.srem(key, members).await.unwrap();
    assert_eq!(1, removed);

    // Removed several values
    let members = vec![
        String::from("player1"),
        String::from("player2"),
        String::from("player3"),
    ];
    let removed = client.srem(key, members).await.unwrap();
    assert_eq!(2, removed);
}

/// `Z`: Add score-member pairs and retrieve score to a single
/// key associated sorted set
#[tokio::test]
async fn zadd_zscore() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let score = client.zscore(key, "player1").await.unwrap();
    assert_eq!(Some(1), score);

    let score = client.zscore("unknown_key", "player1").await.unwrap();
    assert_eq!(None, score);

    let score = client.zscore(key, "unknown_player").await.unwrap();
    assert_eq!(None, score);
}

/// `Z`: Add and retrieve a score-member pair to a single key associated sorted set
#[tokio::test]
async fn zadd_zrange_one_value() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key1";
    let entries = vec![(5, String::from("player1"))];

    let added = client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(1, added);

    let entries_resp = client.zrange(key, 5, 5, false, None, None).await.unwrap();
    assert_eq!(entries.clone(), entries_resp);

    let entries_resp = client.zrange(key, 0, 5, false, None, None).await.unwrap();
    assert_eq!(entries.clone(), entries_resp);

    let entries_resp = client.zrange(key, 0, 10, false, None, None).await.unwrap();
    assert_eq!(entries.clone(), entries_resp);
}

/// `Z`: Add and remove score-member pairs to a single key associated sorted set
#[tokio::test]
async fn zadd_zrem() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key1";
    let entries = vec![(5, String::from("player1"))];

    let added = client.zadd(key, entries).await.unwrap();
    assert_eq!(1, added);

    let members = vec![String::from("player1")];
    let removed = client.zrem(key, members).await.unwrap();
    assert_eq!(1, removed);

    let entries = vec![
        (1, String::from("player1")),
        (2, String::from("player2")),
        (3, String::from("player3")),
    ];

    let added = client.zadd(key, entries).await.unwrap();
    assert_eq!(3, added);

    let members = vec![String::from("player1"), String::from("player2")];
    let removed = client.zrem(key, members.clone()).await.unwrap();
    assert_eq!(2, removed);

    // Try remove a second time.
    let removed = client.zrem(key, members).await.unwrap();
    assert_eq!(0, removed);
}

/// `Z`: Add and retrieve multiple score-member pairs to a
/// single key associated sorted set
#[tokio::test]
async fn zadd_zrange_several_values() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let entries_resp = client.zrange(key, 0, 0, false, None, None).await.unwrap();
    assert_eq!(0, entries_resp.len());

    let entries_resp = client
        .zrange("doesnt_exist", 0, 15, false, None, None)
        .await
        .unwrap();
    assert_eq!(0, entries_resp.len());

    let entries_resp = client.zrange(key, 0, 15, false, None, None).await.unwrap();
    assert_eq!(entries, entries_resp);

    let entries_resp = client.zrange(key, 5, 15, false, None, None).await.unwrap();
    assert_eq!(
        vec![(5, String::from("player2")), (10, String::from("player3")),],
        entries_resp
    );
}

/// `Z`: Test optional arguments REV and LIMIT
#[tokio::test]
async fn zadd_zrange_optional_arguments() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let entries_resp = client.zrange(key, 0, 10, true, None, None).await.unwrap();
    assert_eq!(
        entries_resp,
        entries
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<(u64, String)>>()
    );

    let entries_resp = client
        .zrange(key, 0, 15, true, Some(1), Some(2))
        .await
        .unwrap();
    assert_eq!(
        entries_resp,
        vec![(5, String::from("player2")), (1, String::from("player1"))]
    );

    // Count limit higher than number of elements
    let entries_resp = client
        .zrange(key, 0, 15, true, Some(1), Some(10))
        .await
        .unwrap();
    assert_eq!(
        entries_resp,
        vec![(5, String::from("player2")), (1, String::from("player1"))]
    );
}

/// `Z`: Test zank
#[tokio::test]
async fn zadd_zank() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let rank = client.zrank(key, "player1", false).await.unwrap();
    assert_eq!(Some(0), rank);

    let rank = client.zrank(key, "player1", true).await.unwrap();
    assert_eq!(Some(2), rank);

    let rank = client.zrank(key, "player3", true).await.unwrap();
    assert_eq!(Some(0), rank);

    let rank = client.zrank(key, "unkwown_player", true).await.unwrap();
    assert_eq!(None, rank);
}

/// similar to the "hello world" style test, But this time
/// a single channel subscription will be tested instead
#[tokio::test]
async fn receive_message_subscribed_channel() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into()]).await.unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"world", &message.content[..])
}

/// test that a client gets messages from multiple subscribed channels
#[tokio::test]
async fn receive_message_multiple_subscribed_channels() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message1 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message1.channel);
    assert_eq!(b"world", &message1.content[..]);

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("world", "howdy?".into()).await.unwrap()
    });

    let message2 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("world", &message2.channel);
    assert_eq!(b"howdy?", &message2.content[..])
}

/// test that a client accurately removes its own subscribed channel list
/// when unsubscribing to all subscribed channels by submitting an empty vec
#[tokio::test]
async fn unsubscribes_from_channels() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    subscriber.unsubscribe(&[]).await.unwrap();
    assert_eq!(subscriber.get_subscribed().len(), 0);
}

async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let appendonly_aof = "appendonly_test.aof";
    let warmup = None;

    let handle = tokio::spawn(async move {
        server::run(listener, tokio::signal::ctrl_c(), appendonly_aof, warmup).await
    });

    (addr, handle)
}
