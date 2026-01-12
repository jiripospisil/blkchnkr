use std::alloc::{self, Layout, handle_alloc_error};

use anyhow::{Context, Ok, Result};

use crate::util::page_size;

pub struct IoBuffers {
    layout: Layout,
    elem_size: usize,
    ptr: *mut u8,
}

impl IoBuffers {
    pub fn new(max_io_buf_bytes: u32, queue_depth: u16) -> Result<Self> {
        let page_size = page_size()?;
        let elem_size = (max_io_buf_bytes as usize)
            .checked_next_multiple_of(page_size)
            .context("Invalid elem size.")?;
        let size = elem_size * queue_depth as usize;
        let layout = Layout::from_size_align(size, page_size)?;

        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }

        Ok(Self {
            layout,
            elem_size,
            ptr,
        })
    }

    #[inline(always)]
    pub fn get_buf_addr(&self, tag: u8) -> u64 {
        self.get_buf(tag).addr() as _
    }

    #[inline(always)]
    pub fn get_buf_with_offsets(
        &mut self,
        tag: u8,
        buf_offset: u32,
        curr_offset: u32,
    ) -> *mut u8 {
        unsafe {
            self.get_buf_with_offset(tag, buf_offset)
                .add(curr_offset as usize)
        }
    }

    #[inline(always)]
    fn get_buf_with_offset(
        &mut self,
        tag: u8,
        buf_offset: u32,
    ) -> *mut u8 {
        unsafe { self.get_buf(tag).add((buf_offset as usize) << 9) }
    }

    #[inline(always)]
    fn get_buf(&self, tag: u8) -> *mut u8 {
        unsafe { self.ptr.add(self.elem_size * tag as usize) }
    }
}

impl Drop for IoBuffers {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.ptr, self.layout) };
    }
}
