//! Commands of the operating system (`\ls`, `system "ls"`), and the directory they run in (`\cd`).
use crate::engine::{files, Engine};
use crate::error::{KError, KResult};
use crate::types::*;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(windows)]
fn system_shell(command: &str) -> Command {
    use std::os::windows::process::CommandExt;
    let mut shell = Command::new("cmd");
    // (`cmd` reads its command line its own way: it goes as it is, between the quotes that `/S` takes off)
    shell.raw_arg(format!("/D /S /C \"{command}\""));
    shell
}

#[cfg(not(windows))]
fn system_shell(command: &str) -> Command {
    let mut shell = Command::new("sh");
    shell.arg("-c").arg(command);
    shell
}

impl Engine<'_> {
    /// The directory that relative paths are taken from: the workspace's, else the process's.
    pub(crate) fn directory(&self) -> KResult<PathBuf> {
        Ok(match &self.ws.data_dir {
            Some(dir) => dir.clone(),
            None => std::env::current_dir()?,
        })
    }

    /// Run `command` in the system's shell (`cmd` on Windows, else `sh`) and give the lines it prints. What it prints
    /// as errors goes to the console. The workspace waits until it ends.
    pub(crate) fn shell(&mut self, command: &str) -> KResult<K> {
        // (a copy of the workspace runs nothing: the statement may be run again)
        self.ws.writable()?;
        if !self.ws.host.shell.load(Ordering::SeqCst) {
            return Err(KError::new("access", "shell commands are off: a process that serves clients needs --allow-shell"));
        }
        let output = system_shell(command).current_dir(self.directory()?).stdin(Stdio::null()).stderr(Stdio::inherit()).output()?;
        if !output.status.success() {
            return Err(KError::new("os", format!("the command failed ({})", output.status)));
        }
        Ok(K::Vector(ColVec::General(Attr::None, Arc::new(files::lines(&output.stdout)))))
    }
}
