use miniredis::{
    clients::{BufferedClient, Client},
    server,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A basic "hello world" style test. A server instance is started in a
/// background task. A client instance is then established and used to initialize
/// the buffer. Set and get commands are sent to the server. The response is
/// then evaluated.
#[tokio::test]
async fn pool_key_value_get_set() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    buffered_client.set("hello", "world".into()).await.unwrap();

    let value = buffered_client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

/// Simple set() & len() call to test len()
#[tokio::test]
async fn set_len() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let len = buffered_client.len().await.unwrap();
    assert_eq!(0, len);

    buffered_client.set("foo1", "bar1".into()).await.unwrap();
    buffered_client.set("foo2", "bar2".into()).await.unwrap();

    let len = buffered_client.len().await.unwrap();
    assert_eq!(2, len);
}

/// A delete test with single delete. Try to delete an existing and non
/// existing key
#[tokio::test]
async fn single_delete() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    buffered_client.set("foo", "bar".into()).await.unwrap();

    let deleted = buffered_client.del(vec!["foo".into()]).await.unwrap();
    assert_eq!(1, deleted);

    let deleted = buffered_client
        .del(vec!["unknown_key".into()])
        .await
        .unwrap();
    assert_eq!(0, deleted);
}

/// A delete test for several deletes:
/// - Two existing keys
/// - One existing, one none existing key
/// - Two no existing keys
#[tokio::test]
async fn several_deletes() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    buffered_client.set("foo1", "bar1".into()).await.unwrap();
    buffered_client.set("foo2", "bar2".into()).await.unwrap();

    let members = vec![String::from("foo1"), String::from("foo2")];

    let cnt = buffered_client.del(members.clone()).await.unwrap();
    assert_eq!(2, cnt);

    buffered_client.set("foo1", "bar1".into()).await.unwrap();

    let cnt = buffered_client.del(members.clone()).await.unwrap();
    assert_eq!(1, cnt);

    let cnt = buffered_client.del(members).await.unwrap();
    assert_eq!(0, cnt);
}

/// `S`: Add and retrieve length of underlying set.
#[tokio::test]
async fn sadd_slength() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key1";
    let members = vec![String::from("player1")];

    let added = buffered_client.sadd(key, members.clone()).await.unwrap();
    assert_eq!(1, added);
    let added = buffered_client.sadd(key, members).await.unwrap();
    assert_eq!(0, added);

    let length = buffered_client.slength(key).await.unwrap();
    assert_eq!(1, length);

    let members = vec![String::from("player2"), String::from("player3")];
    let added = buffered_client.sadd(key, members).await.unwrap();

    assert_eq!(2, added);

    let length = buffered_client.slength(key).await.unwrap();
    assert_eq!(3, length);
}

/// `S`: Add and ismember of underlying set.
#[tokio::test]
async fn sadd_sismember() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key1";
    let members = vec![String::from("player2"), String::from("player3")];
    buffered_client.sadd(key, members).await.unwrap();

    let not_member = buffered_client.sismember(key, "player1").await.unwrap();
    assert_eq!(0, not_member);

    let is_member = buffered_client.sismember(key, "player2").await.unwrap();
    assert_eq!(1, is_member);
}

/// `S`: Add and rem of underlying set.
#[tokio::test]
async fn sadd_srem() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key1";
    let members = vec![
        String::from("player1"),
        String::from("player2"),
        String::from("player3"),
    ];
    buffered_client.sadd(key, members).await.unwrap();

    // Removed nothing
    let members = vec![String::from("player4")];
    let removed = buffered_client.srem(key, members).await.unwrap();
    assert_eq!(0, removed);

    // Removed one value
    let members = vec![String::from("player1")];
    let removed = buffered_client.srem(key, members).await.unwrap();
    assert_eq!(1, removed);

    // Removed several values
    let members = vec![
        String::from("player1"),
        String::from("player2"),
        String::from("player3"),
    ];
    let removed = buffered_client.srem(key, members).await.unwrap();
    assert_eq!(2, removed);
}

/// `Z`: Add and retrieve a score-member pair to a single key associated sorted set
#[tokio::test]
async fn zadd_zrange_one_value() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key1";
    let entries = vec![(5, String::from("player1"))];

    let added = buffered_client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(1, added);

    let entries_resp = buffered_client
        .zrange(key, 5, 5, false, None, None)
        .await
        .unwrap();
    assert_eq!(entries.clone(), entries_resp);

    let entries_resp = buffered_client
        .zrange(key, 0, 5, false, None, None)
        .await
        .unwrap();
    assert_eq!(entries.clone(), entries_resp);

    let entries_resp = buffered_client
        .zrange(key, 0, 10, false, None, None)
        .await
        .unwrap();
    assert_eq!(entries.clone(), entries_resp);
}

/// `Z`: Add and remove score-member pairs to a single key associated sorted set
#[tokio::test]
async fn zadd_zrem() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key1";
    let entries = vec![(5, String::from("player1"))];

    let added = buffered_client.zadd(key, entries).await.unwrap();
    assert_eq!(1, added);

    let members = vec![String::from("player1")];
    let removed = buffered_client.zrem(key, members).await.unwrap();
    assert_eq!(1, removed);

    let entries = vec![
        (1, String::from("player1")),
        (2, String::from("player2")),
        (3, String::from("player3")),
    ];

    let added = buffered_client.zadd(key, entries).await.unwrap();
    assert_eq!(3, added);

    let members = vec![String::from("player1"), String::from("player2")];
    let removed = buffered_client.zrem(key, members.clone()).await.unwrap();
    assert_eq!(2, removed);

    // Try remove a second time.
    let removed = buffered_client.zrem(key, members).await.unwrap();
    assert_eq!(0, removed);
}

/// `Z`: Add score-member pairs and retrieve score to a single
/// key associated sorted set
#[tokio::test]
async fn zadd_zscore() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = buffered_client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let score = buffered_client.zscore(key, "player1").await.unwrap();
    assert_eq!(Some(1), score);

    let score = buffered_client
        .zscore("unknown_key", "player1")
        .await
        .unwrap();
    assert_eq!(None, score);

    let score = buffered_client.zscore(key, "unknown_player").await.unwrap();
    assert_eq!(None, score);
}

/// `Z`: Add and retrieve multiple score-member pairs to a
/// single key associated sorted set
#[tokio::test]
async fn zadd_zrange_several_values() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = buffered_client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let entries_resp = buffered_client
        .zrange(key, 0, 0, false, None, None)
        .await
        .unwrap();
    assert_eq!(0, entries_resp.len());

    let entries_resp = buffered_client
        .zrange("doesnt_exist", 0, 15, false, None, None)
        .await
        .unwrap();
    assert_eq!(0, entries_resp.len());

    let entries_resp = buffered_client
        .zrange(key, 0, 15, false, None, None)
        .await
        .unwrap();
    assert_eq!(entries, entries_resp);

    let entries_resp = buffered_client
        .zrange(key, 5, 15, false, None, None)
        .await
        .unwrap();
    assert_eq!(
        vec![(5, String::from("player2")), (10, String::from("player3")),],
        entries_resp
    );
}

/// `Z`: Test optional arguments REV and LIMIT
#[tokio::test]
async fn zadd_zrange_optional_arguments() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = buffered_client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let entries_resp = buffered_client
        .zrange(key, 0, 10, true, None, None)
        .await
        .unwrap();
    assert_eq!(
        entries_resp,
        entries
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<(u64, String)>>()
    );

    let entries_resp = buffered_client
        .zrange(key, 0, 15, true, Some(1), Some(2))
        .await
        .unwrap();
    assert_eq!(
        entries_resp,
        vec![(5, String::from("player2")), (1, String::from("player1"))]
    );

    // Count limit higher than number of elements
    let entries_resp = buffered_client
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

    let client = Client::connect(addr).await.unwrap();
    let buffered_client = BufferedClient::buffer(client);

    let key = "key";
    let entries = vec![
        (1, String::from("player1")),
        (5, String::from("player2")),
        (10, String::from("player3")),
    ];

    let added = buffered_client.zadd(key, entries.clone()).await.unwrap();
    assert_eq!(3, added);

    let rank = buffered_client.zrank(key, "player1", false).await.unwrap();
    assert_eq!(Some(0), rank);

    let rank = buffered_client.zrank(key, "player1", true).await.unwrap();
    assert_eq!(Some(2), rank);

    let rank = buffered_client.zrank(key, "player3", true).await.unwrap();
    assert_eq!(Some(0), rank);

    let rank = buffered_client
        .zrank(key, "unkwown_player", true)
        .await
        .unwrap();
    assert_eq!(None, rank);
}

async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let aof_filename = "appendonly_test.aof";
    let warmup = None;

    let handle = tokio::spawn(async move {
        server::run(listener, tokio::signal::ctrl_c(), aof_filename, warmup).await
    });

    (addr, handle)
}
