//! Adding shards without being asked: while some shard holds more than a set amount of memory, a program of the
//! operator's is run to start another shard, which is then added ([`crate::resize`]). Shards are never removed.
use crate::Cluster;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// When to add a shard, and how to get one.
#[derive(Clone, Debug)]
pub struct Autoscale {
    /// A program that is run with one argument, the `--id` of the shard wanted. It starts that shard, empty, and prints
    /// its address (comma-separated, for replicas) as its last line.
    pub command: PathBuf,
    /// A shard whose variables hold more bytes than this calls for another shard.
    pub above_bytes: usize,
    pub max_shards: usize,
    /// The time between checks.
    pub interval: Duration,
    /// How many checks in a row must find a shard above the mark.
    pub checks: u32,
    /// The least time between the end of one attempt to add a shard and the next.
    pub cooldown: Duration,
    /// How long the program may take, and then the shard it names to answer.
    pub start_limit: Duration,
    /// Say what would be done, and do nothing.
    pub dry_run: bool,
}

impl Autoscale {
    /// Check every 30 s, act on 3 checks in a row, and wait 5 minutes between attempts.
    pub fn new(command: impl Into<PathBuf>, above_bytes: usize, max_shards: usize) -> Self {
        let (interval, cooldown) = (Duration::from_secs(30), Duration::from_secs(300));
        Self { command: command.into(), above_bytes, max_shards, interval, checks: 3, cooldown, start_limit: cooldown, dry_run: false }
    }
}

/// What the cluster's metrics say about it.
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) added: AtomicU64,
    pub(crate) failed: AtomicU64,
    /// Adding stopped helping: nothing more is added until no shard is above the mark.
    pub(crate) paused: AtomicBool,
}

/// The thread that watches the shards; dropping this stops it, once a change of shards under way has ended.
pub struct Autoscaler {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Autoscaler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

fn say(message: impl std::fmt::Display) {
    eprintln!("kix-cluster: autoscale: {message}");
}

#[derive(Default)]
struct Watcher {
    /// Checks in a row that found a shard above the mark.
    over: u32,
    /// No attempt is made before this.
    after: Option<Instant>,
    /// A shard that was started but not added, by its `--id`: it is tried again before another is started.
    started: Option<(usize, String)>,
}

impl Watcher {
    fn check(&mut self, cluster: &Cluster, policy: &Autoscale, stop: &AtomicBool) {
        // (a node that does not know all its shards, or cannot hear from one, does not act)
        if cluster.inner.settle().is_err() {
            return;
        }
        let Ok(held) = cluster.status().into_iter().map(|s| s.map(|s| s.memory_bytes)).collect::<Result<Vec<_>, _>>() else {
            self.over = 0;
            return;
        };
        let counters = &cluster.inner.scaling;
        let Some((full, &bytes)) = held.iter().enumerate().max_by_key(|s| s.1).filter(|s| *s.1 > policy.above_bytes) else {
            self.over = 0;
            counters.paused.store(false, Ordering::SeqCst);
            return;
        };
        self.over += 1;
        let shards = held.len();
        if self.over < policy.checks || counters.paused.load(Ordering::SeqCst) || self.after.is_some_and(|t| Instant::now() < t) {
            return;
        }
        let found = format!("shard {full} holds {} MB", bytes >> 20);
        self.over = 0;
        if shards >= policy.max_shards {
            say(format!("{found}, but there are already {shards} shards, the most allowed"));
        } else if policy.dry_run {
            say(format!("{found}: would add shard {shards}"));
        } else {
            say(format!("{found}: adding shard {shards}"));
            match self.add(cluster, policy, shards, stop) {
                Ok(()) => {
                    counters.added.fetch_add(1, Ordering::SeqCst);
                    let now = cluster.status().into_iter().nth(full).and_then(Result::ok).map_or(bytes, |s| s.memory_bytes);
                    say(format!("shard {shards} added; shard {full} now holds {} MB", now >> 20));
                    // an even share of what moved is 1/(shards + 1) of the data: far less means it cannot be split
                    if bytes.saturating_sub(now) < bytes / (4 * (shards + 1)) {
                        counters.paused.store(true, Ordering::SeqCst);
                        say(format!("that freed little of shard {full} (one value holds many rows, or its tables are replicated): no more shards until none is above the mark"));
                    }
                }
                Err(e) => {
                    counters.failed.fetch_add(1, Ordering::SeqCst);
                    say(format!("shard {shards} was not added: {e}"));
                }
            }
        }
        self.after = Instant::now().checked_add(policy.cooldown);
    }

    /// Start shard `id`, unless one that was started is still to be added, wait for it to answer, and add it.
    fn add(&mut self, cluster: &Cluster, policy: &Autoscale, id: usize, stop: &AtomicBool) -> Result<(), String> {
        let address = match &self.started {
            Some((started, address)) if *started == id => address.clone(),
            _ => start_shard(policy, id, stop)?,
        };
        self.started = Some((id, address.clone()));
        let shard = cluster.inner.new_shard(id as u32, &address).map_err(|e| e.to_string())?;
        let deadline = Instant::now() + policy.start_limit;
        while let Err(e) = cluster.inner.wait(shard.status()) {
            if Instant::now() >= deadline || stop.load(Ordering::SeqCst) {
                return Err(format!("the shard at {address} does not answer: {e}"));
            }
            std::thread::park_timeout(Duration::from_millis(200));
        }
        cluster.add_shard(&address).map_err(|e| e.to_string())?;
        self.started = None;
        Ok(())
    }
}

/// Run the program for shard `id` and give the last line it prints.
fn start_shard(policy: &Autoscale, id: usize, stop: &AtomicBool) -> Result<String, String> {
    let program = policy.command.display();
    let mut command = Command::new(&policy.command);
    let mut child = command.arg(id.to_string()).stdin(Stdio::null()).stdout(Stdio::piped()).spawn().map_err(|e| format!("cannot run {program}: {e}"))?;
    let (lines, printed) = mpsc::channel();
    if let Some(output) = child.stdout.take() {
        // a shard that the program leaves running may keep the output open: it is read for as long as it is
        std::thread::spawn(move || BufReader::new(output).lines().map_while(Result::ok).for_each(|line| drop(lines.send(line))));
    }
    let deadline = Instant::now() + policy.start_limit;
    let status = loop {
        match child.try_wait().map_err(|e| e.to_string())? {
            Some(status) => break status,
            None if Instant::now() >= deadline || stop.load(Ordering::SeqCst) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{program} did not end in time"));
            }
            None => std::thread::park_timeout(Duration::from_millis(50)),
        }
    };
    if !status.success() {
        return Err(format!("{program} failed ({status})"));
    }
    // (what it printed is all there within moments of its end)
    let until = Instant::now() + Duration::from_millis(500);
    let mut address = None;
    while let Ok(line) = printed.recv_timeout(until.saturating_duration_since(Instant::now())) {
        if !line.trim().is_empty() {
            address = Some(line.trim().to_string());
        }
    }
    address.ok_or_else(|| format!("{program} printed no address"))
}

impl Cluster {
    /// Watch the shards and add more as `policy` says, until what this returns is dropped.
    pub fn autoscale(&self, policy: Autoscale) -> Autoscaler {
        let stop = Arc::new(AtomicBool::new(false));
        let (cluster, stopping) = (self.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            let mut watcher = Watcher::default();
            while !stopping.load(Ordering::SeqCst) {
                watcher.check(&cluster, &policy, &stopping);
                let next = Instant::now() + policy.interval;
                while !stopping.load(Ordering::SeqCst) && Instant::now() < next {
                    std::thread::park_timeout(next.saturating_duration_since(Instant::now()));
                }
            }
        });
        Autoscaler { stop, thread: Some(thread) }
    }
}
