use std::fs::create_dir;

use anyhow::{Context, Result, anyhow, bail};

use crate::util::set_fsids;
use crate::{cli::Init, config::Config};

pub fn run(init: Init) -> Result<()> {
    if init.config.exists() {
        bail!(
            "There's already a repository at {}",
            init.config.repository.to_string_lossy()
        );
    }

    set_fsids(&init.config);
    create_repository_dir(&init.config)?;
    create_chunks_dir(&init.config)?;

    init.config.save()?;

    info!(
        "Created a new repository at {}",
        init.config.repository.to_string_lossy()
    );

    Ok(())
}

fn create_repository_dir(config: &Config) -> Result<()> {
    create_dir(&config.repository).with_context(|| {
        anyhow!(
            "Failed to create the repository directory at {}.",
            config.repository.to_string_lossy()
        )
    })
}

fn create_chunks_dir(config: &Config) -> Result<()> {
    let chunks_path = {
        let mut config = config.repository.clone();
        config.push("chunks");
        config
    };

    create_dir(&chunks_path).with_context(|| {
        anyhow!(
            "Failed to create the chunks directory at {}.",
            chunks_path.to_string_lossy()
        )
    })
}
