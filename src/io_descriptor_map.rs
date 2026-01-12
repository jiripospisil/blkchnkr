use std::num::NonZero;
use std::os::fd::BorrowedFd;
use std::{ffi::c_void, ops::Index, os::fd::RawFd};

use anyhow::{Result, anyhow};
use nix::libc;
use nix::sys::mman::{self, MapFlags, ProtFlags};

use crate::bindings::{
    UBLK_MAX_QUEUE_DEPTH, UBLKSRV_CMD_BUF_OFFSET, ublksrv_io_desc,
};
use crate::util::page_size;

#[inline(always)]
fn len(depth: u16) -> Result<usize> {
    let io_size = depth as usize * size_of::<ublksrv_io_desc>();
    let page_size = page_size()?;

    Ok(io_size.next_multiple_of(page_size))
}

#[inline(always)]
fn offset(queue_id: usize) -> Result<usize> {
    let max_len = len(UBLK_MAX_QUEUE_DEPTH as u16)?;
    Ok(UBLKSRV_CMD_BUF_OFFSET as usize + max_len * queue_id)
}

#[derive(Debug)]
pub struct IoDescriptorMap {
    ptr: *mut c_void,
    len: usize,
}

impl IoDescriptorMap {
    pub fn new(
        queue_id: usize,
        ublkc_dev_fd: RawFd,
        queue_depth: u16,
    ) -> Result<Self> {
        let len = NonZero::new(len(queue_depth)?)
            .ok_or_else(|| anyhow!("Incorrect len size"))?;
        let offset = offset(queue_id)?;

        let ptr = unsafe {
            mman::mmap(
                None,
                len,
                ProtFlags::PROT_READ,
                MapFlags::MAP_SHARED | MapFlags::MAP_POPULATE,
                BorrowedFd::borrow_raw(ublkc_dev_fd),
                offset as i64,
            )?
            .as_ptr()
        };

        Ok(Self {
            ptr,
            len: len.into(),
        })
    }
}

impl Index<usize> for IoDescriptorMap {
    type Output = ublksrv_io_desc;

    fn index(&self, index: usize) -> &Self::Output {
        unsafe {
            let desc =
                self.ptr.add(index * size_of::<ublksrv_io_desc>()).cast();

            &*desc
        }
    }
}

impl Drop for IoDescriptorMap {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr, self.len) };
    }
}
