use anyhow::Result;

use crate::cli::Expand;
use crate::config::Config;

pub fn run(expand: Expand) -> Result<()> {
    let mut config = Config::from_repository(expand.repository)?;

    config.expand_size_by_bytes(expand.bytes)?;
    config.save()?;

    info!(
        "Expanded the size of the device to {} ({}B). Restart the server to take effect.",
        size_to_human(config.size),
        config.size
    );

    Ok(())
}

fn size_to_human(size: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

    let mut size = size as f64;

    for unit in UNITS {
        if size < 1024.0 {
            return format!("{:.2}{}", size, unit);
        }
        size /= 1024.0;
    }

    unreachable!()
}
