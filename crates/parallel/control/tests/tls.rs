//! Mutual TLS between the nodes: a cluster with certificates answers as one engine does, and anything without a
//! certificate of the cluster's authority is refused.
mod common;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Empty;
use common::{certificates, load_market, same, Pair, Shards};
use kix_control::Cluster;
use kix_shared_ces::tls::{NodeTls, NODE_NAME};
use kix_storage::{spawn_on, ReplicaConfig, ShardNode};
use kixdb::Server;
use std::time::{Duration, Instant};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

#[test]
fn a_secured_cluster_answers_queries_and_moves_rows_between_shards() {
    let dir = tempfile::tempdir().unwrap();
    let [node] = certificates(dir.path(), ["node"]);
    let mut p = load_market(Pair::with_tls(3, Some(node)));
    p.plan("select from trade where price > 110", "merged in order");
    p.plan("select sum size, avg price by ex from trade", "two-phase");
    // shards send rows to one another: to every shard, and to the shard that owns each row
    p.plan("select sum size by desk from trade ij acct", "broadcast acct");
    p.cluster.set_broadcast_limit(1);
    p.plan("select sum size by desk from trade lj acct", "shuffle trade by trader");
    p.plan("update price: 2 * price from `trade where sym = `IBM", "run on every shard");
    p.check("exec sum price from trade");
    assert!(p.cluster.status().into_iter().all(|s| s.is_ok()));
}

/// Whether a control node using `tls` gets an answer from the shards.
fn reaches(shards: &Shards, tls: Option<NodeTls>) -> bool {
    let cluster = Cluster::connect_with(&shards.config(), tls).unwrap();
    cluster.wait_for_shards(Duration::ZERO).is_ok()
}

#[test]
fn only_holders_of_the_authoritys_certificates_get_through() {
    let (ours, theirs) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let [node] = certificates(ours.path(), ["node"]);
    let [stranger] = certificates(theirs.path(), ["stranger"]);
    let shards = Shards::start_with(1, Some(node.clone()));
    assert!(reaches(&shards, Some(node.clone())));
    // unprotected, and another authority's certificate
    assert!(!reaches(&shards, None));
    assert!(!reaches(&shards, Some(stranger.clone())));
    // a caller that believes our shard but shows a stranger's certificate
    let mixed = NodeTls::from_files(ours.path().join("ca.pem"), theirs.path().join("stranger.pem"), theirs.path().join("stranger.key"));
    assert!(!reaches(&shards, Some(mixed.unwrap())));
    // a caller that believes our shard and shows no certificate at all
    let authority = Certificate::from_pem(std::fs::read(ours.path().join("ca.pem")).unwrap());
    let anonymous = ClientTlsConfig::new().ca_certificate(authority).domain_name(NODE_NAME);
    let address = format!("https://{}", shards.nodes[0].addr);
    let answered = shards.runtime.block_on(async {
        let endpoint = Endpoint::from_shared(address).unwrap().tls_config(anonymous).unwrap();
        match endpoint.connect().await {
            Ok(channel) => FlightServiceClient::new(channel).list_actions(Empty {}).await.is_ok(),
            Err(_) => false,
        }
    });
    assert!(!answered);
    // and a control node does not talk to a shard that is not of its cluster
    let impostor = Shards::start_with(1, Some(stranger));
    assert!(!reaches(&impostor, Some(node.clone())));
    assert!(!reaches(&Shards::start(1), Some(node)));
}

#[test]
fn replicas_replicate_over_mutual_tls() {
    let dir = tempfile::tempdir().unwrap();
    let [node] = certificates(dir.path(), ["node"]);
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let listeners: Vec<_> = (0..3).map(|_| runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap()).collect();
    let addrs: Vec<String> = listeners.iter().map(|l| l.local_addr().unwrap().to_string()).collect();
    let replicas: Vec<_> = (1u64..)
        .zip(listeners)
        .map(|(id, listener)| {
            let peers = (1u64..).zip(addrs.clone()).filter(|p| p.0 != id).collect();
            let mut config = ReplicaConfig::new(id, peers, dir.path().join(format!("raft{id}")));
            config.tls = Some(node.clone());
            let server = Server::open(dir.path().join(format!("data{id}"))).unwrap();
            let _in_runtime = runtime.enter();
            let shard = ShardNode::replicated(0, server, config).unwrap();
            runtime.block_on(spawn_on(shard, listener)).unwrap()
        })
        .collect();
    let shard = kix_shared_ces::ShardInfo { id: 0, addr: addrs.join(",") };
    let cluster = Cluster::connect_with(&kix_shared_ces::ClusterConfig { shards: vec![shard] }, Some(node)).unwrap();
    let mut session = cluster.session();
    // a leader can only be elected, and a change committed, if the replicas reach one another
    let give_up = Instant::now() + Duration::from_secs(20);
    while let Err(e) = session.eval("t: ([] a: 1 2 3)") {
        assert!(Instant::now() < give_up, "{e}");
        std::thread::sleep(Duration::from_millis(100));
    }
    let expected = Server::new().connect().eval("([] a: 1 2 3)").unwrap();
    for replica in &replicas {
        while !replica.server.connect().eval("t").is_ok_and(|t| same(&t, &expected)) {
            assert!(Instant::now() < give_up, "a replica did not get the table");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    drop(cluster);
    for replica in replicas {
        runtime.block_on(replica.shutdown());
    }
}
