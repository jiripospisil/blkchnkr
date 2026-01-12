use std::{
    fs::{self},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    str::FromStr,
};

use anyhow::Result;
use nix::libc;

#[derive(Debug, Clone)]
pub struct QueueLimits {
    pub logical_block_size: u16,
    pub physical_block_size: u16,

    pub minimum_io_size: u16,
    pub optimal_io_size: u16,

    pub dma_alignment: u16,

    pub write_cache: bool,
    pub fua: bool,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            logical_block_size: 512,
            physical_block_size: 4096,

            minimum_io_size: 512,
            optimal_io_size: 4096,

            dma_alignment: 511,

            write_cache: true,
            fua: false,
        }
    }
}

/// Gets queue limits from the device underlying the config file.
pub fn limits_from_device(config_path: &PathBuf) -> Result<QueueLimits> {
    let metadata = fs::metadata(config_path)?;
    let dev_id = metadata.dev();

    let major = libc::major(dev_id);
    let minor = libc::minor(dev_id);

    // The backing device is virtual (or presented as such). Do not bother
    // for now. This unfortunately includes btrfs.
    if major == 0 {
        return Ok(QueueLimits::default());
    }

    // Let's pretend the sysfs is actually mounted at /sys.
    let dir = format!("/sys/dev/block/{}:{}/queue", major, minor);

    macro_rules! read_int_limit {
        ($limits:expr, $dir:expr, $name:ident) => {
            $limits.$name = read_int_limit(&$dir, stringify!($name))
                .unwrap_or($limits.$name);
        };
    }

    macro_rules! match_limit {
        ($limits:expr, $dir:expr, $name:ident, $exp:expr) => {
            $limits.$name = read_str_limit(&$dir, stringify!($name))
                .map(|val| val.trim() == $exp)
                .unwrap_or($limits.$name);
        };
    }

    let mut limits = QueueLimits::default();

    read_int_limit!(limits, dir, logical_block_size);
    read_int_limit!(limits, dir, physical_block_size);
    read_int_limit!(limits, dir, minimum_io_size);
    read_int_limit!(limits, dir, optimal_io_size);
    read_int_limit!(limits, dir, dma_alignment);

    match_limit!(limits, dir, write_cache, "write back");
    match_limit!(limits, dir, fua, "1");

    Ok(limits)
}

fn read_int_limit<T: FromStr>(dir: &String, file_name: &str) -> Option<T> {
    let fullpath = format!("{}/{}", dir, file_name);

    let content = fs::read_to_string(fullpath).ok()?;
    let limit = T::from_str(content.trim()).ok()?;

    Some(limit)
}

fn read_str_limit(dir: &String, file_name: &str) -> Option<String> {
    let fullpath = format!("{}/{}", dir, file_name);
    fs::read_to_string(fullpath).ok()
}
