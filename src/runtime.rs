use anyhow::{Result, bail};
use io_uring::squeue;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind::Interrupted;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll::{self, Pending, Ready};
use std::task::{Context, Waker};

use crate::types::Ring;

pub struct Runtime {
    ring: Rc<RefCell<Ring>>,
    mailbox: Rc<RefCell<HashMap<u64, i32>>>,
    tasks: HashMap<u8, Pin<Box<dyn Future<Output = ()>>>>,
}

impl Runtime {
    pub fn new(ring: Ring) -> Self {
        let capacity = ring.params().sq_entries() as usize;

        Self {
            ring: Rc::new(RefCell::new(ring)),
            mailbox: Rc::new(RefCell::new(HashMap::new())),
            tasks: HashMap::with_capacity(capacity),
        }
    }

    pub fn spawn<T, F>(&mut self, tag: u8, to_fut: T)
    where
        T: FnOnce(Submitter) -> F,
        F: Future<Output = ()> + 'static,
    {
        self.tasks.insert(
            tag,
            Box::pin(to_fut(Submitter::new(
                self.ring.clone(),
                self.mailbox.clone(),
                tag,
            ))),
        );
    }

    pub fn run(&mut self) -> Result<()> {
        self.run_all_tasks();

        loop {
            if self.tasks.is_empty() {
                return Ok(());
            }

            self.submit_and_wait()?;
            self.process_entries()?;
            self.run_tasks();
        }
    }

    /// Run all tasks to kick off their state machines.
    fn run_all_tasks(&mut self) {
        let mut cx = Context::from_waker(Waker::noop());

        let tags: SmallVec<[u8; 128]> =
            self.tasks.keys().cloned().collect();

        for tag in tags {
            let task = self.tasks.get_mut(&tag).unwrap();
            if task.as_mut().poll(&mut cx).is_ready() {
                self.tasks.remove(&tag);
            }
        }
    }

    /// Wait for IO events.
    fn submit_and_wait(&mut self) -> Result<()> {
        loop {
            match self.ring.borrow().submit_and_wait(1) {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if err.kind() == Interrupted {
                        continue;
                    }

                    let err: anyhow::Error = err.into();
                    bail!(err.context(
                        "Got an error while waiting for entries."
                    ));
                }
            }
        }
    }

    /// Deliver the messages to tasks' mailboxes based on user data.
    fn process_entries(&mut self) -> Result<()> {
        loop {
            let mut ring = self.ring.borrow_mut();
            let Some(entry) = ring.completion().next() else {
                return Ok(());
            };

            self.mailbox
                .borrow_mut()
                .insert(entry.user_data(), entry.result());
        }
    }

    /// Run only tasks that should be able to make progress.
    fn run_tasks(&mut self) {
        let mut cx = Context::from_waker(Waker::noop());

        let entry_keys: SmallVec<[u64; 128]> =
            self.mailbox.borrow().keys().cloned().collect();

        for entry_key in entry_keys {
            // Strip off the upper bits to get the tag without idx.
            let tag = entry_key as u8;

            if let Some(task) = self.tasks.get_mut(&tag)
                && task.as_mut().poll(&mut cx).is_ready() {
                    self.tasks.remove(&tag);
                }
        }
    }
}

pub struct Submitter {
    ring: Rc<RefCell<Ring>>,
    mailbox: Rc<RefCell<HashMap<u64, i32>>>,
    tag: u8,
    idx: u64,
}

impl Submitter {
    fn new(
        ring: Rc<RefCell<Ring>>,
        mailbox: Rc<RefCell<HashMap<u64, i32>>>,
        tag: u8,
    ) -> Self {
        Self {
            ring,
            mailbox,
            tag,
            idx: 1,
        }
    }

    pub fn submit_entry(
        &mut self,
        mut entry: squeue::Entry,
    ) -> Result<Waiter> {
        let entry_key = self.idx << 8 | (self.tag as u64);
        entry.set_user_data(entry_key);

        let mut ring = self.ring.borrow_mut();
        if ring.submission().is_full() {
            ring.submit()?;
        }
        unsafe { ring.submission().push(&entry)? };

        self.idx = self.idx.wrapping_add(1);
        Ok(Waiter::new(self.mailbox.clone(), entry_key))
    }

    pub fn register_files_update(
        &mut self,
        idx: u32,
        fds: &[RawFd],
    ) -> Result<usize> {
        self.ring
            .borrow_mut()
            .submitter()
            .register_files_update(idx, fds)
            .map_err(Into::into)
    }
}

pub struct Waiter {
    mailbox: Rc<RefCell<HashMap<u64, i32>>>,

    entry_key: u64,
    result: Option<i32>,
}

impl Waiter {
    fn new(
        mailbox: Rc<RefCell<HashMap<u64, i32>>>,
        entry_key: u64,
    ) -> Self {
        Self {
            mailbox,
            entry_key,
            result: None,
        }
    }
}

impl Future for Waiter {
    type Output = i32;

    fn poll(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        match self.result {
            Some(result) => Ready(result),
            None => {
                let mail = {
                    let mut mailbox = self.mailbox.borrow_mut();
                    mailbox.remove(&self.entry_key)
                };

                match mail {
                    Some(result) => {
                        self.as_mut().result = Some(result);
                        Ready(result)
                    }
                    None => Pending,
                }
            }
        }
    }
}
