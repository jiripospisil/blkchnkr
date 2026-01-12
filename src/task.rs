use std::{cell::RefCell, collections::HashMap, os::fd::AsRawFd, rc::Rc};

use crate::{
    bindings::{
        UBLK_IO_OP_FLUSH, UBLK_IO_OP_READ, UBLK_IO_OP_WRITE,
        UBLK_IO_OP_WRITE_ZEROES, ublksrv_io_desc,
    },
    bindings_ext::UBLK_IO_RES_ABORT,
    config::Config,
    io_buffers::IoBuffers,
    io_descriptor_map::IoDescriptorMap,
    io_worker::UBLKC_FD_IDX,
    parts::{Part, parts_for_event},
    runtime::{Submitter, Waiter},
    sqes::{
        create_fetch_req_commit_sqe, create_fetch_req_sqe,
        create_flush_sqe, create_rw_sqe, create_rw_sqe_with_offset,
        create_write_zeroes_sqe,
    },
    util::open_or_create_chunk,
};

use anyhow::{Context, Result, bail};
use nix::libc;
use smallvec::SmallVec;

pub struct Task {
    pub submitter: Submitter,
    pub queue_id: usize,
    pub config: Config,
    pub tag: u8,
    pub descs: Rc<RefCell<IoDescriptorMap>>,
    pub bufs: Rc<RefCell<IoBuffers>>,
    pub file_indexes: Rc<RefCell<HashMap<u32, u32>>>,
}

impl Task {
    pub fn new(
        submitter: Submitter,
        queue_id: usize,
        config: Config,
        tag: u8,
        descs: Rc<RefCell<IoDescriptorMap>>,
        bufs: Rc<RefCell<IoBuffers>>,
        file_indexes: Rc<RefCell<HashMap<u32, u32>>>,
    ) -> Self {
        Self {
            submitter,
            queue_id,
            config,
            tag,
            descs,
            bufs,
            file_indexes,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        debug!("queue_id={} tag={} starting", self.queue_id, self.tag);

        let result = self.send_fetch_req().await?;
        if result < 0 {
            if result == UBLK_IO_RES_ABORT {
                debug!(
                    "queue_id={} tag={} shutting down before doing any work...",
                    self.queue_id, self.tag
                );
                return Ok(());
            }
            bail!("Received an error during initial fetch: {}", result);
        }

        loop {
            debug!(
                "queue_id={} tag={} running loop",
                self.queue_id, self.tag
            );

            let result = match self.process_request().await {
                Ok(res) => {
                    debug!(
                        "queue_id={} tag={} committing result {}",
                        self.queue_id, self.tag, res
                    );
                    self.send_commit_fetch_req(res).await?
                }
                Err(err) => {
                    error!(
                        "queue_id={} tag={} failed to handle request err={}",
                        self.queue_id, self.tag, err
                    );
                    self.send_commit_fetch_req(-libc::EIO).await?
                }
            };

            if result == UBLK_IO_RES_ABORT {
                debug!(
                    "queue_id={} tag={} shutting down...",
                    self.queue_id, self.tag
                );
                return Ok(());
            }

            if result < 0 {
                error!(
                    "queue_id={} tag={} received an error during commit: {}",
                    self.queue_id, self.tag, result
                );
            }
        }
    }

    async fn send_fetch_req(&mut self) -> Result<i32> {
        let sqe = create_fetch_req_sqe(UBLKC_FD_IDX, self);
        Ok(self.submitter.submit_entry(sqe)?.await)
    }

    async fn process_request(&mut self) -> Result<i32> {
        debug!(
            "queue_id={} tag={} processing request",
            self.queue_id, self.tag
        );

        let desc = self.descs.borrow()[self.tag as usize];
        match desc.op() {
            UBLK_IO_OP_READ | UBLK_IO_OP_WRITE => {
                self.process_rw_request(desc.op(), desc).await
            }
            UBLK_IO_OP_FLUSH => self.process_flush_request(desc).await,
            UBLK_IO_OP_WRITE_ZEROES => {
                self.process_write_zeroes_request(desc).await
            }
            _ => {
                bail!(
                    "queue_id={} tag={} unknown request op={}",
                    self.queue_id,
                    self.tag,
                    desc.op()
                )
            }
        }
    }

    async fn process_rw_request(
        &mut self,
        op: u32,
        desc: ublksrv_io_desc,
    ) -> Result<i32> {
        self.log_rw_request(op, &desc);

        // // Start off the entries in parallel. The runtime doesn't wait for the
        // // futures to be awaited.
        let entries = parts_for_event(&self.config, &desc)
            .map(|part| {
                let file_index =
                    self.open_or_create_cached(part.file_num)?;
                let sqe =
                    create_rw_sqe(self, op, file_index, &part, &desc);
                let entry = self.submitter.submit_entry(sqe)?;

                Ok((part, file_index, entry))
            })
            .collect::<Result<SmallVec<[(Part, u32, Waiter); 8]>>>()?;

        debug_assert!(!entries.spilled());

        let mut result_all = 0;

        // Go over each future again to make sure it finished successfully.
        for entry in entries {
            let (part, file_index, mut fut) = entry;
            let mut current = 0;

            loop {
                let result = fut.await;

                debug_assert_ne!(result, 0);

                if result > 0 {
                    current += result as u32;

                    // We got all the data we wanted.
                    if current == part.nr_sectors << 9 {
                        result_all += current;
                        break;
                    }
                }

                // If there's an error other than EINTR, bail out and propagate
                // the error upstream.
                if result < 0 && result != -libc::EINTR {
                    return Ok(result);
                }

                // A short. Try again with adjusted offset.
                let sqe = create_rw_sqe_with_offset(
                    self, op, file_index, &part, &desc, current,
                );
                fut = self.submitter.submit_entry(sqe)?;
            }
        }

        debug_assert_eq!(result_all >> 9, unsafe {
            desc.__bindgen_anon_1.nr_sectors
        });

        Ok(result_all as i32)
    }

    async fn process_flush_request(
        &mut self,
        desc: ublksrv_io_desc,
    ) -> Result<i32> {
        debug!(
            "queue_id={} tag={} processing flush request desc={:?}",
            self.queue_id, self.tag, desc
        );

        let entries = parts_for_event(&self.config, &desc)
            .map(|part| {
                let file_index =
                    self.open_or_create_cached(part.file_num)?;
                let sqe = create_flush_sqe(file_index);
                let entry = self.submitter.submit_entry(sqe)?;

                Ok((file_index, entry))
            })
            .collect::<Result<SmallVec<[(u32, Waiter); 8]>>>()?;

        debug_assert!(!entries.spilled());

        for entry in entries {
            let (file_index, mut fut) = entry;

            loop {
                let result = fut.await;

                if result == 0 {
                    break;
                }

                if result < 0 && result != -libc::EINTR {
                    return Ok(result);
                }

                let sqe = create_flush_sqe(file_index);
                fut = self.submitter.submit_entry(sqe)?;
            }
        }

        Ok(0)
    }

    async fn process_write_zeroes_request(
        &mut self,
        desc: ublksrv_io_desc,
    ) -> Result<i32> {
        debug!(
            "queue_id={} tag={} processing write zeroes request desc={:?}",
            self.queue_id, self.tag, desc
        );

        let entries = parts_for_event(&self.config, &desc)
            .map(|part| {
                let file_index =
                    self.open_or_create_cached(part.file_num)?;
                let sqe =
                    create_write_zeroes_sqe(file_index, &part, &desc);
                let entry = self.submitter.submit_entry(sqe)?;

                Ok((part, file_index, entry))
            })
            .collect::<Result<SmallVec<[(Part, u32, Waiter); 8]>>>()?;

        debug_assert!(!entries.spilled());

        for entry in entries {
            let (part, file_index, mut fut) = entry;

            loop {
                let result = fut.await;

                if result == 0 {
                    break;
                }

                if result < 0 && result != -libc::EINTR {
                    return Ok(result);
                }

                let sqe =
                    create_write_zeroes_sqe(file_index, &part, &desc);
                fut = self.submitter.submit_entry(sqe)?;
            }
        }

        Ok(0)
    }

    async fn send_commit_fetch_req(&mut self, result: i32) -> Result<i32> {
        let sqe = create_fetch_req_commit_sqe(UBLKC_FD_IDX, self, result);
        Ok(self.submitter.submit_entry(sqe)?.await)
    }

    fn open_or_create_cached(&mut self, file_index: u32) -> Result<u32> {
        let mut file_indexes = self.file_indexes.borrow_mut();

        if let Some(idx) = file_indexes.get(&file_index) {
            return Ok(*idx);
        }

        let file = open_or_create_chunk(&self.config, file_index)?;
        let idx = (file_indexes.len() + 1) as u32;

        self.submitter
            .register_files_update(idx, &[file.as_raw_fd()])
            .context("Failed to register more file descriptors.")?;
        file_indexes.insert(file_index, idx);

        Ok(idx)
    }

    #[inline(always)]
    #[allow(unused)]
    fn log_rw_request(&self, op: u32, desc: &ublksrv_io_desc) {
        let op = if op == UBLK_IO_OP_READ {
            "read"
        } else {
            "write"
        };

        debug!(
            "queue_id={} tag={} processing {} request desc={:?}",
            self.queue_id, self.tag, op, desc
        );
    }
}
