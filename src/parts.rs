use crate::{bindings::ublksrv_io_desc, config::Config};

#[derive(Debug)]
pub struct Parts {
    chunk_size: u64,
    start_sector: u64,
    nr_sectors: u32,
    buf_offset: u32,
}

#[derive(Debug)]
pub struct Part {
    pub file_num: u32,
    pub nr_sectors: u32,
    pub start_sector: u64,
    pub buf_offset: u32,
}

impl Iterator for Parts {
    type Item = Part;

    fn next(&mut self) -> Option<Self::Item> {
        if self.nr_sectors == 0 {
            return None;
        }

        let start_within_chunk = self.start_sector % self.chunk_size;
        let left_in_chunk = (self.chunk_size - start_within_chunk) as u32;
        let to_read_from_chunk = left_in_chunk.min(self.nr_sectors);

        let part = Part {
            file_num: (self.start_sector / self.chunk_size) as u32,
            start_sector: start_within_chunk,
            nr_sectors: to_read_from_chunk,
            buf_offset: self.buf_offset,
        };

        self.start_sector += to_read_from_chunk as u64;
        self.nr_sectors -= to_read_from_chunk;
        self.buf_offset += to_read_from_chunk;

        Some(part)
    }
}

pub fn parts_for_event(
    config: &Config,
    desc: &ublksrv_io_desc,
) -> Parts {
    Parts {
        chunk_size: config.chunk_size / 512,
        start_sector: desc.start_sector,
        nr_sectors: unsafe { desc.__bindgen_anon_1.nr_sectors },
        buf_offset: 0,
    }
}
