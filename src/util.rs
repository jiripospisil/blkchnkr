use std::{
    fs::{self, File, OpenOptions, create_dir_all},
    io,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use nix::{
    libc,
    unistd::{self, SysconfVar},
};

use crate::config::Config;

pub fn page_size() -> Result<usize> {
    let page_size = unistd::sysconf(SysconfVar::PAGE_SIZE)?;

    page_size
        .ok_or_else(|| anyhow!("Cannot determine page size"))
        .map(|s| s as usize)
}

pub fn set_fsids(config: &Config) {
    if let Some(fsuid) = config.fsuid {
        unistd::setfsuid(fsuid.into());
    }

    if let Some(fsgid) = config.fsgid {
        unistd::setfsgid(fsgid.into());
    }
}

// This is executed once per chunk per thread. All operations should be
// idempotent.
pub fn open_or_create_chunk(
    config: &Config,
    file_index: u32,
) -> Result<File> {
    let filepath = build_filepath(config, file_index)?;

    mkdir(filepath.parent().unwrap())?;

    let flags = if config.direct_io() {
        libc::O_DIRECT
    } else {
        0
    };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(flags)
        .open(filepath)
        .context("Failed to open/create chunk.")?;

    ftruncate(config, &file)?;

    Ok(file)
}

fn build_filepath(config: &Config, file_index: u32) -> Result<PathBuf> {
    let mut path = fs::canonicalize(&config.repository)?;

    path.push("chunks");
    path.push(format!("{:02x}", file_index % 256));
    path.push(file_index.to_string());

    Ok(path)
}

fn mkdir(subdir: &Path) -> Result<()> {
    create_dir_all(subdir).with_context(|| {
        anyhow!("Failed to create a directory for a chunk.")
    })
}

fn ftruncate(config: &Config, file: &File) -> Result<()> {
    loop {
        let res = unsafe {
            libc::ftruncate(file.as_raw_fd(), config.chunk_size as i64)
        };

        if res < 0 {
            if let Some(err) = io::Error::last_os_error().raw_os_error() {
                if err == libc::EINTR {
                    continue;
                }

                bail!("ftruncate failed: {}", err);
            }

            bail!("ftruncate failed with unknown error");
        }

        return Ok(());
    }
}
