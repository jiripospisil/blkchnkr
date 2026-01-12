use std::fs::OpenOptions;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::process;
use std::thread::{self, JoinHandle, sleep};
use std::time::Duration;
use std::{env, io};

use anyhow::{Context, Result, bail};
use io_uring::opcode::PollAdd;
use io_uring::types::{Fd, Fixed};
use nix::libc;
use nix::sys::resource::{self, Resource};
use nix::sys::signal::{self, SigSet, SigmaskHow, Signal};
use nix::sys::signalfd::{SfdFlags, SignalFd};

use crate::bindings::{
    UBLK_ATTR_FUA, UBLK_ATTR_VOLATILE_CACHE, UBLK_F_USER_RECOVERY,
    UBLK_PARAM_TYPE_DMA_ALIGN, UBLK_S_DEV_DEAD, UBLK_S_DEV_FAIL_IO,
    UBLK_S_DEV_LIVE, UBLK_S_DEV_QUIESCED, ublk_param_dma_align,
};
use crate::bindings::{
    UBLK_PARAM_TYPE_BASIC, ublk_param_basic, ublk_params,
    ublksrv_ctrl_dev_info,
};
use crate::config::Config;
use crate::io_worker::IoWorker;
use crate::sqes::{
    send_add_dev_cmd, send_del_dev_cmd, send_get_info_cmd,
    send_set_params_cmd, send_start_recover_dev_cmd,
    send_start_recovery_cmd, send_stop_dev_cmd,
};
use crate::types::{AddResult, Ring128};

use crate::cli::Start;
use crate::util::set_fsids;

const UBLK_CONTROL_FD_IDX: Fixed = Fixed(0);

fn set_io_flusher() {
    const PR_SET_IO_FLUSHER: libc::c_int = 57;

    if unsafe { libc::prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0) } < 0 {
        let err: anyhow::Error = io::Error::last_os_error().into();

        warn!(
            "Unable to set PR_SET_IO_FLUSHER for the process. This *may* \
            lead to deadlocks in very low memory situations. Grant blkchnkr \
            the CAP_SYS_RESOURCE capability or run it as root (~CAP_SYS_ADMIN). Or \
            just never run out of memory. Err: {}",
            err
        );
    }
}

fn set_rlimit_nofile() {
    const OUGHT_TO_BE_ENOUGH: u64 = 400_000;

    match resource::getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft_limit, hard_limit)) => {
            let soft_limit = soft_limit.max(OUGHT_TO_BE_ENOUGH);
            let hard_limit = hard_limit.max(OUGHT_TO_BE_ENOUGH);

            if let Err(err) = resource::setrlimit(
                Resource::RLIMIT_NOFILE,
                soft_limit,
                hard_limit,
            ) {
                warn!(
                    "Unable to change the current limit on open file descriptors to {}. \
                    Depending on chunk size, blkchnkr might need to open a lot files \
                    per thread and this might fail if the limit is too low. Err: {}",
                    OUGHT_TO_BE_ENOUGH, err
                );
            }
        }
        Err(err) => {
            warn!(
                "Unable to figure out the current limit on open file descriptors. \
                Depending on chunk size, blkchnkr might need to open a lot files \
                per thread and this might fail if the limit is too low. Err: {}",
                err
            );
        }
    };
}

#[inline(always)]
fn open_ublk_ctrl() -> Result<OwnedFd> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/ublk-control")
        .context(
            "Unable to open /dev/ublk-control. Make sure the kernel \
            module ublk_drv is loaded and accessible to the current user.",
        )?;

    Ok(file.into())
}

fn open_ublkc_dev(dev_id: u32) -> Result<OwnedFd> {
    // It might take a while before the device shows up.
    let path = format!("/dev/ublkc{}", dev_id);

    for _ in 0..3 {
        let file = OpenOptions::new().read(true).write(true).open(&path);

        match file {
            Ok(file) => return Ok(file.into()),
            Err(err) => match err.kind() {
                io::ErrorKind::NotFound
                | io::ErrorKind::ResourceBusy
                | io::ErrorKind::Interrupted => {}
                _ => {
                    bail!("Unable to open {}, err: {}", path, err);
                }
            },
        }

        sleep(Duration::from_millis(150));
    }

    bail!("Unable to open {}", path)
}

fn create_ctrl_ring(fd: OwnedFd) -> Result<Ring128> {
    let ring = Ring128::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .build(8)?;

    // UBLK_CONTROL_FD_IDX
    ring.submitter().register_files(&[fd.as_raw_fd()])?;

    Ok(ring)
}

fn add_new_dev(
    config: &Config,
    ring: &mut Ring128,
) -> Result<(bool, ublksrv_ctrl_dev_info)> {
    let dev_info = ublksrv_ctrl_dev_info {
        dev_id: config.dev_id(),
        nr_hw_queues: config.threads()?,
        max_io_buf_bytes: 512 << 11,
        queue_depth: 128,
        flags: UBLK_F_USER_RECOVERY.into(),
        ..Default::default()
    };

    match send_add_dev_cmd(dev_info, ring, UBLK_CONTROL_FD_IDX) {
        Ok(result) => match result {
            AddResult::NewDevice(dev_info) => Ok((true, dev_info)),
            AddResult::AttemptRecovery => attempt_recovery(config, ring),
        },
        Err(err) => Err(err),
    }
}

fn attempt_recovery(
    config: &Config,
    ring: &mut Ring128,
) -> Result<(bool, ublksrv_ctrl_dev_info)> {
    let dev_info =
        send_get_info_cmd(config.dev_id(), ring, UBLK_CONTROL_FD_IDX)?;

    match dev_info.state as u32 {
        UBLK_S_DEV_QUIESCED | UBLK_S_DEV_FAIL_IO => {} // attempt recovery

        UBLK_S_DEV_LIVE => bail!(
            "A running device with ID {} already exists.",
            dev_info.dev_id
        ),

        UBLK_S_DEV_DEAD => bail!(
            "A dead device with ID {} already exists and cannot be recovered.",
            dev_info.dev_id
        ),

        _ => unreachable!("Unknown server state"),
    }

    send_start_recovery_cmd(dev_info, ring, UBLK_CONTROL_FD_IDX)?;

    Ok((false, dev_info))
}

fn dev_attrs(config: &mut Config) -> Result<u32> {
    let mut attrs = 0;

    if config.write_cache()? {
        attrs |= UBLK_ATTR_VOLATILE_CACHE;
    }

    if config.fua()? {
        attrs |= UBLK_ATTR_FUA;
    }

    Ok(attrs)
}

fn set_dev_params(
    config: &mut Config,
    dev_info: &ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
) -> Result<()> {
    let params = ublk_params {
        len: ublk_params::len() as _,
        types: UBLK_PARAM_TYPE_BASIC | UBLK_PARAM_TYPE_DMA_ALIGN,
        basic: ublk_param_basic {
            attrs: dev_attrs(config)?,
            logical_bs_shift: config.logical_bs_shift()?,
            physical_bs_shift: config.physical_bs_shift()?,
            io_min_shift: config.io_min_shift()?,
            io_opt_shift: config.io_opt_shift()?,
            max_sectors: dev_info.max_io_buf_bytes >> 9,
            dev_sectors: config.size >> 9,
            ..Default::default()
        },
        dma: ublk_param_dma_align {
            alignment: config.dma_alignment()?,
            ..Default::default()
        },
        ..Default::default()
    };

    send_set_params_cmd(dev_info, params, ring, UBLK_CONTROL_FD_IDX)
}

fn setup_signals() -> Result<OwnedFd> {
    let mut set = SigSet::empty();
    set.add(Signal::SIGINT);
    set.add(Signal::SIGTERM);

    signal::sigprocmask(SigmaskHow::SIG_BLOCK, Some(&set), None)?;

    Ok(SignalFd::with_flags(&set, SfdFlags::SFD_NONBLOCK)?.into())
}

fn handle_sigint(ring: &mut Ring128, fd: OwnedFd) -> Result<()> {
    let sqe = PollAdd::new(Fd(fd.as_raw_fd()), libc::POLLIN as _).build();
    let sqe = sqe.user_data(42);

    unsafe { ring.submission().push(&sqe.into())? };
    ring.submit_and_wait(1)?;

    if let Some(cqe) = ring.completion().next() {
        if cqe.user_data() != 42 {
            bail!("Unexpected message.");
        }

        if cqe.result() < 0 {
            error!(
                "Polling failed with err {}. Shutting down anyway.",
                cqe.result()
            );
        }
    }

    Ok(())
}

fn start_worker_threads(
    config: &Config,
    dev_info: &ublksrv_ctrl_dev_info,
    ublkc_dev_fd: &OwnedFd,
) -> Result<Box<[JoinHandle<()>]>> {
    let mut queue_threads =
        Vec::with_capacity(dev_info.nr_hw_queues.into());

    let dev_info = *dev_info;
    let ublkc_dev_fd = ublkc_dev_fd.as_raw_fd();

    for i in 0..dev_info.nr_hw_queues as usize {
        let config = config.clone();

        queue_threads.push(
            thread::Builder::new()
                .name(format!("tid={} worker", i))
                .spawn(move || {
                    worker_thread_fn(i, config, dev_info, ublkc_dev_fd);
                })?,
        );
    }

    Ok(queue_threads.into_boxed_slice())
}

fn worker_thread_fn(
    queue_id: usize,
    config: Config,
    dev_info: ublksrv_ctrl_dev_info,
    ublkc_dev_fd: RawFd,
) {
    debug!("online");

    match IoWorker::new(queue_id, config, dev_info, ublkc_dev_fd) {
        Ok(mut worker) => {
            if let Err(err) = worker.work() {
                error!("Worker crashed. Err: {err}");
            }
        }
        Err(err) => error!("Failed to initialize worker. Err: {err}"),
    }
}

#[inline(always)]
fn join_worker_threads(worker_threads: Box<[JoinHandle<()>]>) {
    worker_threads.into_iter().for_each(|t| _ = t.join());
}

pub fn run(start: Start) -> Result<()> {
    info!("Starting up (v{})", env!("CARGO_PKG_VERSION"));

    let mut config = Config::from_repository(start.repository)?;

    set_io_flusher();
    set_rlimit_nofile();

    let fd = open_ublk_ctrl()?;
    let mut ring = create_ctrl_ring(fd)?;

    let (is_new_device, dev_info) = add_new_dev(&config, &mut ring)?;

    if is_new_device {
        set_dev_params(&mut config, &dev_info, &mut ring)?;
    }

    debug!("dev_info={:#?}", dev_info);

    // Set the filesystem ids only after we've opened and setup the
    // devices.
    set_fsids(&config);

    // Close the fd gracefully on exit. Set up the signals here such that
    // the block is inherited by worker threads.
    let signal_fd = setup_signals()?;
    let ublkc_dev_fd = open_ublkc_dev(dev_info.dev_id)?;

    let worker_threads =
        start_worker_threads(&config, &dev_info, &ublkc_dev_fd)?;

    send_start_recover_dev_cmd(
        is_new_device,
        &dev_info,
        &mut ring,
        process::id(),
        UBLK_CONTROL_FD_IDX,
    )?;

    if is_new_device {
        info!(
            "Created a new block device at /dev/ublkb{}",
            dev_info.dev_id
        );
    } else {
        info!(
            "Recovered the block device at /dev/ublkb{}",
            dev_info.dev_id
        );
    }
    info!("Ready!");

    handle_sigint(&mut ring, signal_fd)?;

    info!("Stopping...");
    send_stop_dev_cmd(&dev_info, &mut ring, UBLK_CONTROL_FD_IDX)?;

    debug!("Waiting for all threads to finish...");
    join_worker_threads(worker_threads);

    debug!("Deleting the device...");
    send_del_dev_cmd(&dev_info, &mut ring, UBLK_CONTROL_FD_IDX)?;

    info!("Bye");

    Ok(())
}
