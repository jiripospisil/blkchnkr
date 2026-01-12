use std::{
    error::Error,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    str::FromStr,
    thread,
};

use anyhow::{Context, Ok, Result, anyhow, bail};

use crate::queue_limits::{QueueLimits, limits_from_device};

#[derive(Debug, Clone)]
pub struct Config {
    /// The version of the config
    pub version: u8,

    /// The path to the repository
    pub repository: PathBuf,

    /// The id of the device. Allocates a new one if not specified.
    pub dev_id: Option<u32>,

    /// The total size of the repository in bytes.
    pub size: u64,

    /// The size of one individual chunk in bytes.
    pub chunk_size: u64,

    /// The number of threads handling IO.
    pub threads: Option<u16>,

    /// The user owner of newly created chunks.
    pub fsuid: Option<u32>,

    /// The group owner of newly created chunks.
    pub fsgid: Option<u32>,

    /// Use direct IO.
    pub direct_io: Option<bool>,

    /// The underlying device's queue limits. Loaded on demand.
    pub queue_limits: Option<QueueLimits>,
}

impl Config {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repository: PathBuf,
        dev_id: Option<u32>,
        size: u64,
        chunk_size: u64,
        threads: Option<u16>,
        fsuid: Option<u32>,
        fsgid: Option<u32>,
        direct_io: Option<bool>,
    ) -> Self {
        Self {
            version: 1,
            repository,
            dev_id,
            size,
            chunk_size,
            threads,
            fsuid,
            fsgid,
            direct_io,
            queue_limits: None,
        }
    }

    pub fn from_repository(repository: PathBuf) -> Result<Self> {
        #[allow(clippy::let_and_return)]
        let config = read_config_file(repository);
        debug!("{:#?}", config);
        config
    }

    pub fn expand_size_by_bytes(&mut self, bytes: u64) -> Result<()> {
        self.size = self
            .size
            .checked_add(bytes)
            .and_then(|size| {
                size.checked_next_multiple_of(self.chunk_size)
            })
            .context("Invalid final size.")?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id.unwrap_or(u32::MAX)
    }

    pub fn threads(&self) -> Result<u16> {
        if let Some(threads) = self.threads {
            Ok(threads)
        } else {
            Ok(nr_cpu()?)
        }
    }

    pub fn direct_io(&self) -> bool {
        self.direct_io.unwrap_or_default()
    }

    pub fn logical_bs_shift(&mut self) -> Result<u8> {
        Ok(self.queue_limits()?.logical_block_size.ilog2() as _)
    }

    pub fn physical_bs_shift(&mut self) -> Result<u8> {
        Ok(self.queue_limits()?.physical_block_size.ilog2() as _)
    }

    pub fn io_min_shift(&mut self) -> Result<u8> {
        Ok(self.queue_limits()?.minimum_io_size.ilog2() as _)
    }

    pub fn io_opt_shift(&mut self) -> Result<u8> {
        let size = self.queue_limits()?.optimal_io_size;
        Ok(if size == 0 { 0 } else { size.ilog2() } as _)
    }

    pub fn dma_alignment(&mut self) -> Result<u32> {
        Ok(self.queue_limits()?.dma_alignment as _)
    }

    pub fn write_cache(&mut self) -> Result<bool> {
        Ok(self.queue_limits()?.write_cache)
    }

    pub fn fua(&mut self) -> Result<bool> {
        Ok(self.queue_limits()?.fua)
    }

    pub fn exists(&self) -> bool {
        self.config_path().is_file()
    }

    pub fn save(&self) -> Result<()> {
        let config_path = self.config_path();

        let mut f = File::create(&config_path)?;
        f.write_all(self.to_string().as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    fn config_path(&self) -> PathBuf {
        let mut config = self.repository.clone();
        config.push("config");
        config
    }

    fn queue_limits(&mut self) -> Result<&QueueLimits> {
        if self.queue_limits.is_none() {
            self.queue_limits =
                Some(limits_from_device(&self.config_path())?);
        }

        Ok(self.queue_limits.as_ref().unwrap())
    }
}

impl Config {
    #[allow(clippy::inherent_to_string)]
    fn to_string(&self) -> String {
        fn push(text: &mut String, name: &str, value: impl ToString) {
            text.push_str(name);
            text.push(' ');
            text.push_str(&value.to_string());
            text.push('\n');
        }

        fn push_opt(
            text: &mut String,
            name: &str,
            value: Option<impl ToString>,
        ) {
            if let Some(value) = value {
                push(text, name, value);
            }
        }

        let mut text = String::with_capacity(128);

        push(&mut text, "version", 1);
        push_opt(&mut text, "dev-id", self.dev_id);
        push(&mut text, "size", self.size);
        push(&mut text, "chunk-size", self.chunk_size);

        push_opt(&mut text, "fsuid", self.fsuid);
        push_opt(&mut text, "fsgid", self.fsgid);
        push_opt(&mut text, "direct-io", self.direct_io);

        text
    }
}

fn read_config_file(repository: PathBuf) -> Result<Config> {
    let config_path = {
        let mut clone = repository.clone();
        clone.push("config");
        clone
    };

    let config_str =
        fs::read_to_string(&config_path).with_context(|| {
            anyhow!(
                "Could not read the repository's config file at {}",
                config_path.to_str().expect("A valid unicode path.")
            )
        })?;

    let mut version: Option<u8> = None;
    let mut dev_id: Option<u32> = None;
    let mut size: Option<u64> = None;
    let mut chunk_size: Option<u64> = None;
    let mut threads: Option<u16> = None;
    let mut fsuid: Option<u32> = None;
    let mut fsgid: Option<u32> = None;
    let mut direct_io: Option<bool> = None;

    for line in config_str.lines() {
        if line.starts_with("#") {
            continue;
        }

        let mut split = line.splitn(2, " ");

        let name = split
            .next()
            .ok_or_else(|| anyhow!("Invalid setting name"))?;

        let value = split
            .next()
            .ok_or_else(|| anyhow!("Invalid setting value"))?;

        match name {
            "version" => version = Some(parse_num("version", value)?),
            "dev-id" => dev_id = Some(parse_num("dev-id", value)?),
            "size" => size = Some(parse_num("size", value)?),
            "chunk-size" => {
                chunk_size = Some(parse_num("chunk-size", value)?)
            }
            "threads" => threads = Some(parse_num("threads", value)?),
            "fsuid" => fsuid = Some(parse_num("fsuid", value)?),
            "fsgid" => fsgid = Some(parse_num("fsgid", value)?),
            "direct-io" => {
                direct_io = Some(parse_bool("direct-io", value)?)
            }
            s => bail!("Unknown config setting \"{}\"", s),
        }
    }

    Ok(Config {
        version: version.ok_or_else(|| {
            anyhow!("Missing version in the config file.")
        })?,
        repository,
        dev_id,
        size: size
            .ok_or_else(|| anyhow!("Missing size in the config file."))?,
        chunk_size: chunk_size.ok_or_else(|| {
            anyhow!("Missing chunk-size in the config file.")
        })?,
        threads,
        fsuid,
        fsgid,
        direct_io,
        queue_limits: None,
    })
}

fn parse_num<T: FromStr>(label: &str, num: &str) -> Result<T>
where
    <T as FromStr>::Err: Error + Send + Sync + 'static, // 🤦
{
    T::from_str(num)
        .with_context(|| anyhow!("Invalid value for {}", label))
}

fn parse_bool(label: &str, num: &str) -> Result<bool> {
    match num {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        _ => bail!("Invalid value for {}", label),
    }
}

fn nr_cpu() -> Result<u16> {
    // This doesn't account for a number of conditions (masks, quotas, ...)
    // but it's good enough. Set the number of threads explicitly if not.
    Ok(thread::available_parallelism()
        .with_context(|| {
            anyhow!("Could not determine the number of threads.")
        })?
        .get() as _)
}
