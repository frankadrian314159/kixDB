//! A shard that stops answering does not hold the cluster for ever.
mod common;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use kix_control::client::ticket;
use kix_control::Cluster;
use kix_shared_ces::wire::{self, ShuffleReq, Work};
use kix_shared_ces::{ClusterConfig, ShardInfo};
use kixdb::K;
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[test]
fn a_request_a_stuck_shard_does_not_answer_fails() {
    let shards = common::Shards::start(2);
    let cluster = Cluster::connect(&shards.config()).unwrap();
    cluster.set_request_timeout(Duration::from_secs(1));
    let mut s = cluster.session();
    s.eval("t: ([] sym: `a`b`c`d; v: 1 2 3 4)").unwrap();
    // something on the first shard holds its engine and does not let go
    let server = shards.nodes[0].server.clone();
    let (release, held) = mpsc::channel::<()>();
    let (holding, now_held) = mpsc::channel();
    let hold = std::thread::spawn(move || {
        server.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    // the statement, and the one waiting behind it, fail instead of waiting
    let start = Instant::now();
    for statement in ["`t insert (`e; 5)", "count t"] {
        let e = s.eval(statement).unwrap_err();
        assert!(e.signal == "cluster" && e.detail.contains("shard 0") && e.detail.contains("no answer within 1s"), "{e}");
    }
    assert!(start.elapsed() < Duration::from_secs(10), "{:?}", start.elapsed());
    release.send(()).unwrap();
    hold.join().unwrap();
    assert!(s.eval("count t").is_ok());
}

#[test]
fn a_shard_stops_working_on_a_request_its_caller_gave_up() {
    let shards = common::Shards::start(1);
    // a loop that would take far longer than anyone waits
    let work = ticket(Work::Q("n: 0; do[2000000000; n+: 1]".into())).unwrap();
    shards.runtime.block_on(async {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", shards.nodes[0].addr)).unwrap().connect().await.unwrap();
        let mut client = FlightServiceClient::new(channel);
        assert!(tokio::time::timeout(Duration::from_millis(500), client.do_get(work)).await.is_err(), "the loop ended");
    });
    // the engine is free again
    let (tx, rx) = mpsc::channel();
    let server = shards.nodes[0].server.clone();
    std::thread::spawn(move || tx.send(server.connect_internal().eval("n").map(|n| n.to_string())));
    let n = rx.recv_timeout(Duration::from_secs(5)).expect("the engine is still busy").unwrap();
    assert!(n != "2000000000", "{n}");
}

#[test]
fn a_silent_replica_is_given_up_for_another() {
    // takes connections, and never says anything
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = silent.local_addr().unwrap();
    let shards = common::Shards::start(1);
    let alone = Cluster::connect(&ClusterConfig { shards: vec![ShardInfo { id: 0, addr: addr.to_string() }] }).unwrap();
    let start = Instant::now();
    assert_eq!(alone.wait_for_shards(Duration::from_secs(1)), Err(vec![0]));
    assert!(start.elapsed() < Duration::from_secs(5), "{:?}", start.elapsed());
    // with the time limit far off, the silence itself is noticed, and the other replica serves
    let both = format!("{addr},{}", shards.nodes[0].addr);
    let cluster = Cluster::connect(&ClusterConfig { shards: vec![ShardInfo { id: 0, addr: both }] }).unwrap();
    let mut s = cluster.session();
    let start = Instant::now();
    s.eval("t: ([] v: 1 2 3)").unwrap();
    assert_eq!(s.eval("count t").unwrap(), K::Long(3));
    assert!(start.elapsed() < Duration::from_secs(60), "{:?}", start.elapsed());
}

#[test]
fn a_shard_stops_sending_to_a_stuck_one_when_its_caller_gives_up() {
    let shards = common::Shards::start(2);
    for n in &shards.nodes {
        n.server.connect_internal().eval("t: ([] a: til 100); u: 0#t").unwrap();
    }
    let server = shards.nodes[1].server.clone();
    let (release, held) = mpsc::channel::<()>();
    let (holding, now_held) = mpsc::channel();
    let hold = std::thread::spawn(move || {
        server.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    // the first shard is told to send its rows to both, and the caller stops waiting
    let scan = kixdb::parser::parse_script("select from t").unwrap().remove(0);
    let peers = shards.nodes.iter().map(|n| n.addr.to_string()).collect();
    let req = ShuffleReq { scan, key_cols: vec![], target_table: "u".into(), slots: vec![0; 1024], peers, me: 0 };
    let body = postcard::to_allocvec(&req).unwrap();
    shards.runtime.block_on(async {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", shards.nodes[0].addr)).unwrap().connect().await.unwrap();
        let mut client = FlightServiceClient::new(channel);
        let call = client.do_action(Action::new(wire::ACTION_SHUFFLE, body));
        assert!(tokio::time::timeout(Duration::from_secs(2), call).await.is_err(), "the stuck shard answered");
    });
    let busy = || shards.nodes[0].control().metrics().in_flight;
    let start = Instant::now();
    while busy() > 0 {
        assert!(start.elapsed() < Duration::from_secs(5), "the sender still waits");
        std::thread::sleep(Duration::from_millis(20));
    }
    release.send(()).unwrap();
    hold.join().unwrap();
}
