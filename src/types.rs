use io_uring::{IoUring, cqueue, squeue};

use crate::bindings::ublksrv_ctrl_dev_info;

pub type Ring = IoUring<squeue::Entry, cqueue::Entry>;
pub type Ring128 = IoUring<squeue::Entry128, cqueue::Entry32>;

#[derive(Debug)]
pub enum AddResult {
    NewDevice(ublksrv_ctrl_dev_info),
    AttemptRecovery,
}
