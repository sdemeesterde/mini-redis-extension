use mini_redis::{clients::Client, server};
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

/// A delete test with single delete. Try to delete an existing and non
/// existing key
#[tokio::test]
async fn single_delete() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("foo", "bar".into()).await.unwrap();

    let is_present = client.delete("foo").await.unwrap();
    assert!(is_present);

    let is_not_present = client.delete("unknown_key").await.unwrap();
    assert!(!is_not_present);
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

    let cnt = client
        .deletes(&[String::from("foo1"), String::from("foo2")])
        .await
        .unwrap();
    assert_eq!(2, cnt);

    client.set("foo1", "bar1".into()).await.unwrap();

    let cnt = client
        .deletes(&[String::from("foo1"), String::from("foo2")])
        .await
        .unwrap();
    assert_eq!(1, cnt);

    let cnt = client
        .deletes(&[String::from("foo1"), String::from("foo2")])
        .await
        .unwrap();
    assert_eq!(0, cnt);
}

/// Add and retrieve a score-member pair to a single key associated sorted set
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

/// Add and retrieve multiple score-member pairs to a
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
