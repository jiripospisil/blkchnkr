use std::io;
use std::mem::MaybeUninit;
use std::slice;

use anyhow::Result;
use anyhow::bail;
use io_uring::cqueue::Entry32;
use io_uring::opcode::Fallocate;
use io_uring::opcode::Fsync;
use io_uring::opcode::Read;
use io_uring::opcode::UringCmd16;
use io_uring::opcode::Write;
use io_uring::squeue::Entry;
use io_uring::squeue::Entry128;
use io_uring::types::SubmitArgs;
use io_uring::types::Timespec;
use io_uring::{opcode::UringCmd80, types::Fixed};
use nix::libc;

use crate::bindings::UBLK_IO_F_FUA;
use crate::bindings::UBLK_IO_F_NOUNMAP;
use crate::bindings::UBLK_IO_OP_READ;
use crate::bindings::ublksrv_io_cmd;
use crate::bindings::ublksrv_io_desc;
use crate::bindings::{
    ublk_params, ublksrv_ctrl_cmd, ublksrv_ctrl_dev_info,
};
use crate::bindings_ext::UBLK_U_CMD_ADD_DEV;
use crate::bindings_ext::UBLK_U_CMD_DEL_DEV_ASYNC;
use crate::bindings_ext::UBLK_U_CMD_END_USER_RECOVERY;
use crate::bindings_ext::UBLK_U_CMD_GET_DEV_INFO;
use crate::bindings_ext::UBLK_U_CMD_SET_PARAMS;
use crate::bindings_ext::UBLK_U_CMD_START_DEV;
use crate::bindings_ext::UBLK_U_CMD_START_USER_RECOVERY;
use crate::bindings_ext::UBLK_U_CMD_STOP_DEV;
use crate::bindings_ext::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
use crate::bindings_ext::UBLK_U_IO_FETCH_REQ;
use crate::parts::Part;
use crate::task::Task;
use crate::types::AddResult;
use crate::types::Ring128;

fn serialize<T, const N: usize>(cmd: T) -> [u8; N] {
    let size = size_of::<T>();
    debug_assert!(size <= N);

    let ptr = &raw const cmd;
    let slice = unsafe { slice::from_raw_parts(ptr.cast(), size) };

    let mut data = [0u8; N];
    data[0..size].copy_from_slice(slice);

    data
}

fn create_io_cmd_sqe(fd: u32, op: u32, cmd: ublksrv_io_cmd) -> Entry {
    UringCmd16::new(Fixed(fd), op).cmd(serialize(cmd)).build()
}

pub fn create_fetch_req_sqe(fd: u32, task: &Task) -> Entry {
    let mut cmd = ublksrv_io_cmd {
        tag: task.tag as u16,
        q_id: task.queue_id as u16,
        ..Default::default()
    };
    cmd.__bindgen_anon_1.addr = task.bufs.borrow().get_buf_addr(task.tag);

    create_io_cmd_sqe(fd, UBLK_U_IO_FETCH_REQ, cmd)
}

pub fn create_fetch_req_commit_sqe(
    fd: u32,
    task: &Task,
    result: i32,
) -> Entry {
    let mut cmd = ublksrv_io_cmd {
        tag: task.tag as u16,
        q_id: task.queue_id as u16,
        result,
        ..Default::default()
    };
    cmd.__bindgen_anon_1.addr = task.bufs.borrow().get_buf_addr(task.tag);

    create_io_cmd_sqe(fd, UBLK_U_IO_COMMIT_AND_FETCH_REQ, cmd)
}

fn create_ctrl_cmd_sqe(
    fd: Fixed,
    op: u32,
    cmd: ublksrv_ctrl_cmd,
) -> Entry128 {
    UringCmd80::new(fd, op).cmd(serialize(cmd)).build()
}

fn submit_and_wait(ring: &mut Ring128, sqe: Entry128) -> Result<Entry32> {
    unsafe { ring.submission().push(&sqe)? };

    let timespec = Timespec::new().sec(5);
    let args = SubmitArgs::new().timespec(&timespec);

    match ring.submitter().submit_with_args(1, &args) {
        Ok(_) => {
            if let Some(cqe) = ring.completion().next() {
                return Ok(cqe);
            }

            bail!("Failed to receive response from the driver in time.");
        }
        Err(err) => {
            let err: anyhow::Error = err.into();
            bail!(err.context("Got an error while waiting for response."));
        }
    }
}

pub fn send_add_dev_cmd(
    mut dev_info: ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<AddResult> {
    let cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        len: ublksrv_ctrl_dev_info::len(),
        addr: &raw mut dev_info as u64,
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_ADD_DEV, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(AddResult::NewDevice(dev_info)),
        // EEXIST
        -17 => Ok(AddResult::AttemptRecovery),
        res => bail!(
            "Got an error while trying to add the device. Err: {}",
            io::Error::from_raw_os_error(-res)
        ),
    }
}

pub fn send_get_info_cmd(
    dev_id: u32,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<ublksrv_ctrl_dev_info> {
    let mut dev_info = MaybeUninit::<ublksrv_ctrl_dev_info>::uninit();

    let cmd = ublksrv_ctrl_cmd {
        dev_id,
        queue_id: u16::MAX,
        addr: dev_info.as_mut_ptr().addr() as _,
        len: ublksrv_ctrl_dev_info::len(),
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_GET_DEV_INFO, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(unsafe { dev_info.assume_init() }),
        res => bail!(
            "Got an error while trying to get info about the device. Err: {}",
            io::Error::from_raw_os_error(-res)
        ),
    }
}

pub fn send_start_recovery_cmd(
    dev_info: ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<()> {
    let cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_START_USER_RECOVERY, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(()),
        res => bail!(
            "Got an error while trying to start recovery. Err: {}",
            io::Error::from_raw_os_error(-res)
        ),
    }
}

pub fn send_set_params_cmd(
    dev_info: &ublksrv_ctrl_dev_info,
    params: ublk_params,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<()> {
    let cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        len: ublk_params::len(),
        addr: &raw const params as u64,
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_SET_PARAMS, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(()),
        res => {
            bail!("Got unexpected result when setting parameters: {}", res)
        }
    }
}

pub fn send_start_recover_dev_cmd(
    is_new_device: bool,
    dev_info: &ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
    pid: u32,
    fd: Fixed,
) -> Result<()> {
    let mut cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    };
    cmd.data[0] = pid as u64;

    let cmd_op = if is_new_device {
        UBLK_U_CMD_START_DEV
    } else {
        UBLK_U_CMD_END_USER_RECOVERY
    };
    let sqe = create_ctrl_cmd_sqe(fd, cmd_op, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(()),
        res => {
            bail!("Got unexpected result when starting device: {}", res)
        }
    }
}

pub fn send_stop_dev_cmd(
    dev_info: &ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<()> {
    let cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_STOP_DEV, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(()),
        res => {
            bail!("Got unexpected result when stopping device: {}", res)
        }
    }
}

pub fn send_del_dev_cmd(
    dev_info: &ublksrv_ctrl_dev_info,
    ring: &mut Ring128,
    fd: Fixed,
) -> Result<()> {
    let cmd = ublksrv_ctrl_cmd {
        dev_id: dev_info.dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    };
    let sqe = create_ctrl_cmd_sqe(fd, UBLK_U_CMD_DEL_DEV_ASYNC, cmd);

    match submit_and_wait(ring, sqe)?.result() {
        0 => Ok(()),
        res => {
            bail!("Got unexpected result when deleting device: {}", res)
        }
    }
}

pub fn create_rw_sqe(
    task: &Task,
    op: u32,
    file_index: u32,
    part: &Part,
    desc: &ublksrv_io_desc,
) -> Entry {
    create_rw_sqe_with_offset(task, op, file_index, part, desc, 0)
}

pub fn create_rw_sqe_with_offset(
    task: &Task,
    op: u32,
    file_index: u32,
    part: &Part,
    desc: &ublksrv_io_desc,
    curr_offset: u32,
) -> Entry {
    let buf = task.bufs.borrow_mut().get_buf_with_offsets(
        task.tag,
        part.buf_offset,
        curr_offset,
    );

    if op == UBLK_IO_OP_READ {
        Read::new(
            Fixed(file_index),
            buf,
            (part.nr_sectors << 9) - curr_offset,
        )
        .offset((part.start_sector << 9) + curr_offset as u64)
        .build()
    } else {
        Write::new(
            Fixed(file_index),
            buf,
            (part.nr_sectors << 9) - curr_offset,
        )
        .offset((part.start_sector << 9) + curr_offset as u64)
        .rw_flags(fua_flags(desc))
        .build()
    }
}

fn fua_flags(desc: &ublksrv_io_desc) -> i32 {
    if desc.op_flags & UBLK_IO_F_FUA != 0 {
        libc::RWF_DSYNC
    } else {
        0
    }
}

pub fn create_flush_sqe(file_index: u32) -> Entry {
    Fsync::new(Fixed(file_index)).build()
}

pub fn create_write_zeroes_sqe(
    file_index: u32,
    part: &Part,
    desc: &ublksrv_io_desc,
) -> Entry {
    let mode = if desc.flags() & UBLK_IO_F_NOUNMAP > 0 {
        libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_ZERO_RANGE
    } else {
        libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE
    };

    Fallocate::new(Fixed(file_index), (part.nr_sectors as u64) << 9)
        .offset(part.start_sector << 9)
        .mode(mode)
        .build()
}
