use anyhow::Result;

use crate::cli;

pub fn run() -> Result<()> {
    eprintln!("{}", cli::HELP);
    Ok(())
}
