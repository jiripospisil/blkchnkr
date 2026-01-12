use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::RawFd;
use std::rc::Rc;

use crate::bindings::ublksrv_ctrl_dev_info;
use crate::config::Config;
use crate::io_buffers::IoBuffers;
use crate::io_descriptor_map::IoDescriptorMap;
use crate::runtime::Runtime;
use crate::task::Task;
use crate::types::Ring;

use anyhow::Result;

pub const UBLKC_FD_IDX: u32 = 0;

fn create_ring(
    dev_info: &ublksrv_ctrl_dev_info,
    fd: RawFd,
) -> Result<Ring> {
    let ring = Ring::builder()
        // We might need to split the incoming requests into multiple IO
        // requests.
        .setup_cqsize(dev_info.queue_depth as u32 * 3)
        .setup_single_issuer()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .build(dev_info.queue_depth as u32 * 3)?;

    // Ought to be enough for anybody
    ring.submitter().register_files_sparse(400_000)?;

    // UBLKC_FD_IDX = /dev/ublkcN
    ring.submitter().register_files_update(0, &[fd])?;

    Ok(ring)
}

pub struct IoWorker {
    queue_id: usize,
    config: Config,
    dev_info: ublksrv_ctrl_dev_info,

    descriptor_map: Rc<RefCell<IoDescriptorMap>>,
    bufs: Rc<RefCell<IoBuffers>>,
    file_indexes: Rc<RefCell<HashMap<u32, u32>>>,

    runtime: Runtime,
}

impl IoWorker {
    pub fn new(
        queue_id: usize,
        config: Config,
        dev_info: ublksrv_ctrl_dev_info,
        ublkc_dev_fd: RawFd,
    ) -> Result<Self> {
        let descriptor_map = IoDescriptorMap::new(
            queue_id,
            ublkc_dev_fd,
            dev_info.queue_depth,
        )?;
        let bufs = IoBuffers::new(
            dev_info.max_io_buf_bytes,
            dev_info.queue_depth,
        )?;

        let ring = create_ring(&dev_info, ublkc_dev_fd)?;
        let runtime = Runtime::new(ring);

        Ok(Self {
            queue_id,
            config,
            dev_info,

            descriptor_map: Rc::new(RefCell::new(descriptor_map)),
            bufs: Rc::new(RefCell::new(bufs)),
            file_indexes: Rc::new(RefCell::new(HashMap::with_capacity(
                1024,
            ))),

            runtime,
        })
    }

    pub fn work(&mut self) -> Result<()> {
        self.spawn_tasks()?;
        self.runtime.run()
    }

    fn spawn_tasks(&mut self) -> Result<()> {
        debug!("spawning tasks");

        for tag in 0..self.dev_info.queue_depth as u8 {
            let queue_id = self.queue_id;
            let config = self.config.clone();
            let descs = self.descriptor_map.clone();
            let bufs = self.bufs.clone();
            let file_indexes = self.file_indexes.clone();

            self.runtime.spawn(tag, |submitter| async move {
                let mut t = Task::new(
                    submitter,
                    queue_id,
                    config,
                    tag,
                    descs,
                    bufs,
                    file_indexes,
                );

                if let Err(err) = t.run().await {
                    error!("tag={} task failed err={}", tag, err);
                }
            });
        }

        debug!("done spawning tasks");

        Ok(())
    }
}
