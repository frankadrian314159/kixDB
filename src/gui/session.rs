//! Single-session background worker so long `q` scripts or streaming CSV imports keep the GUI responsive.
use crate::backend::Backend;
use kixdb::BatchResult;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::{channel, Receiver, TryRecvError};
use std::time::{Duration, Instant};

type Conn = Box<dyn Backend>;

pub struct Finished {
    pub results: Vec<BatchResult>,
    pub elapsed: Duration,
}

struct Job {
    rx: Receiver<(Conn, Finished)>,
    started: Instant,
}

pub struct Session {
    conn: Option<Conn>,
    job: Option<Job>,
    login: String,
}

impl Session {
    pub fn new(conn: impl Backend + 'static) -> Self {
        let login = conn.login();
        Self {
            conn: Some(Box::new(conn)),
            job: None,
            login,
        }
    }

    pub fn replace(&mut self, conn: impl Backend + 'static) {
        self.login = conn.login();
        self.job = None;
        self.conn = Some(Box::new(conn));
    }

    pub fn is_busy(&self) -> bool {
        self.job.is_some()
    }

    pub fn running_for(&self) -> Option<Duration> {
        self.job.as_ref().map(|j| j.started.elapsed())
    }

    pub fn login(&self) -> String {
        match &self.conn {
            Some(c) => c.login(),
            None => self.login.clone(),
        }
    }

    pub fn start(&mut self, script: String, wake: impl FnOnce() + Send + 'static) {
        self.start_with(move |c| vec![c.execute(&script)], wake);
    }

    pub fn start_with(
        &mut self,
        job: impl FnOnce(&mut dyn Backend) -> Vec<BatchResult> + Send + 'static,
        wake: impl FnOnce() + Send + 'static,
    ) {
        let mut conn = self.conn.take().expect("session is already busy");
        self.login = conn.login();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let t0 = Instant::now();
            let results = job(&mut *conn);
            let _ = tx.send((conn, Finished { results, elapsed: t0.elapsed() }));
            wake();
        });
        self.job = Some(Job { rx, started: Instant::now() });
    }

    pub fn poll(&mut self) -> Option<Finished> {
        let job = self.job.as_ref()?;
        match job.rx.try_recv() {
            Ok((conn, fin)) => {
                self.login = conn.login();
                self.conn = Some(conn);
                self.job = None;
                Some(fin)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.job = None;
                None
            }
        }
    }

    #[allow(dead_code)]
    pub fn wait(&mut self) -> Option<Finished> {
        loop {
            self.job.as_ref()?;
            if let Some(f) = self.poll() {
                return Some(f);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Deref for Session {
    type Target = dyn Backend;
    fn deref(&self) -> &Self::Target {
        self.conn.as_deref().expect("cannot access session while worker is busy")
    }
}

impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_deref_mut().expect("cannot access session while worker is busy")
    }
}
