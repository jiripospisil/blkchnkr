use std::env::Args;
use std::path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;

use crate::config::Config;

pub const HELP: &str = "
blkchnkr is a utility for creating virtual block devices backed by on
demand created chunk-sized files.

Usage:
    blkchnkr --repository <path> <command>

Available commands:

    init        Initializes a new repository at the given path
                (--repository or -r).

                The assigned device ID (/dev/ublkbN) can be specified via
                --dev-id. It's highly recommended to set this to have a
                persistent location. Defaults to the first available ID.

                The size of the device in bytes is required and can be
                specified via --size. The value can be increased (but not
                decreased) later (see expand) The minimum is 256MiB.

                The size of an individual chunk in bytes can be specified
                via --chunk-size and defaults to 512MiB. The larger the
                value the less management under the hood and higher
                performance. The value cannot be changed. The minimum is
                32MiB.

                Supported suffixes: M, G, T.

                The file system owner of all created directories and files
                can be specified via --fsuid and --fsgid. Defaults to the
                effective user running the binary.

                The number of threads handling IO requests can be specified
                via --threads. Defaults roughly to the number of CPUs.


    start       Starts the server at the given path (--repository or -r).

    expand      Expand the size of the device of the given repository
                (--repository or -r) and round up the new size to the
                nearest multiple of the chunk size.

                The number in bytes by which the device should increase in
                size is required and can be specified via --bytes.

                Supported suffixes: M, G, T.

                The server must be restarted for the new size to take
                effect.
";

#[derive(Debug)]
pub struct Version;

#[derive(Debug)]
pub struct Help;

#[derive(Debug)]
pub struct Init {
    pub config: Config,
}

impl Init {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[derive(Debug)]
pub struct Start {
    pub repository: PathBuf,
}

impl Start {
    pub fn new(repository: PathBuf) -> Self {
        Self { repository }
    }
}

#[derive(Debug)]
pub struct Expand {
    pub repository: PathBuf,
    pub bytes: u64,
}

impl Expand {
    pub fn new(repository: PathBuf, bytes: u64) -> Self {
        Self { repository, bytes }
    }
}

#[derive(Debug)]
pub enum Command {
    Version(Version),
    Help(Help),
    Init(Init),
    Start(Start),
    Expand(Expand),
}

pub fn parse_cli(env: Args) -> Result<Command> {
    let mut env = env.peekable();
    env.next().ok_or_else(|| anyhow!("Invalid invocation."))?;

    if env.peek().is_none() {
        return Ok(Command::Help(Help));
    }

    match env.next().as_deref() {
        Some("--version") | Some("-v") => Ok(Command::Version(Version)),
        Some("--help") | Some("-h") => Ok(Command::Help(Help)),
        Some("init") => parse_init(env),
        Some("start") => parse_start(env),
        Some("expand") => parse_expand(env),
        _ => {
            bail!("A valid command is required. See --help.")
        }
    }
}

fn parse_init(mut env: impl Iterator<Item = String>) -> Result<Command> {
    let mut repository: Option<PathBuf> = None;
    let mut dev_id: Option<u32> = None;
    let mut size: Option<u64> = None;
    let mut chunk_size = 512 * 1024 * 1024;
    let mut threads: Option<u16> = None;
    let mut fsuid: Option<u32> = None;
    let mut fsgid: Option<u32> = None;

    loop {
        match env.next().as_deref() {
            Some("--help") | Some("-h") => return Ok(Command::Help(Help)),
            Some("--repository") | Some("-r") => {
                repository = Some(parse_path("--repository", env.next())?);
            }
            Some("--dev-id") => {
                dev_id = Some(parse_num("--dev-id", env.next())?);
            }
            Some("--size") => {
                size = Some(parse_size("--size", env.next())?);
            }
            Some("--chunk-size") => {
                chunk_size = parse_size("--chunk-size", env.next())?;
            }
            Some("--threads") => {
                threads = Some(parse_num("--threads", env.next())? as _);
            }
            Some("--fsuid") => {
                fsuid = Some(parse_num("--fsuid", env.next())?);
            }
            Some("--fsgid") => {
                fsgid = Some(parse_num("--fsgid", env.next())?);
            }
            Some(f) => {
                bail!("Unknown flag {}. See --help.", f);
            }
            None => {
                break;
            }
        };
    }

    let Some(repository) = repository else {
        bail!(
            "The path to the repository (--repository) is required. See --help."
        );
    };

    let Some(size) = size else {
        bail!(
            "The size of the repository (--size) is required. See --help."
        );
    };

    let mib = 1024 * 1024;

    if size < 256 * mib {
        bail!(
            "The size of the repository (--size) must be at least 256MiB."
        );
    }

    if chunk_size < 32 * mib {
        bail!("The chunk size (--chunk-size) must be at least 32MiB.");
    }

    let chunk_size = chunk_size
        .checked_next_multiple_of(4096)
        .context("Invalid final check size")?;

    let size = size
        .checked_next_multiple_of(chunk_size)
        .context("Invalid final size.")?;

    Ok(Command::Init(Init::new(Config::new(
        repository, dev_id, size, chunk_size, threads, fsuid, fsgid, None,
    ))))
}

fn parse_start(mut env: impl Iterator<Item = String>) -> Result<Command> {
    let mut repository: Option<PathBuf> = None;

    loop {
        match env.next().as_deref() {
            Some("--help") | Some("-h") => return Ok(Command::Help(Help)),
            Some("--repository") | Some("-r") => {
                repository = Some(parse_path("--repository", env.next())?);
            }
            Some(f) => {
                bail!("Unknown flag {}. See --help.", f);
            }
            None => {
                break;
            }
        };
    }

    let Some(repository) = repository else {
        bail!(
            "The path to the repository (--repository) is required. See --help."
        );
    };

    Ok(Command::Start(Start::new(repository)))
}

fn parse_expand(mut env: impl Iterator<Item = String>) -> Result<Command> {
    let mut repository: Option<PathBuf> = None;
    let mut bytes: Option<u64> = None;

    loop {
        match env.next().as_deref() {
            Some("--help") | Some("-h") => return Ok(Command::Help(Help)),
            Some("--repository") | Some("-r") => {
                repository = Some(parse_path("--repository", env.next())?);
            }
            Some("--bytes") => {
                bytes = Some(parse_size("--bytes", env.next())?);
            }
            Some(f) => {
                bail!("Unknown flag {}. See --help.", f);
            }
            None => {
                break;
            }
        };
    }

    let Some(repository) = repository else {
        bail!(
            "The path to the repository (--repository) is required. See --help."
        );
    };

    let Some(bytes) = bytes else {
        bail!("The number of bytes (--bytes) is required. See --help.");
    };

    Ok(Command::Expand(Expand::new(repository, bytes)))
}

fn parse_path(label: &str, val: Option<String>) -> Result<PathBuf> {
    let Some(val) = val else {
        bail!("Missing {} value.", label);
    };

    let pathbuf = path::absolute(val)
        .with_context(|| anyhow!("Invalid {} path.", label))?;

    Ok(pathbuf)
}

fn parse_size(label: &str, val: Option<String>) -> Result<u64> {
    let Some(mut val) = val else {
        bail!("Missing {} value.", label);
    };

    let mul: u64 = match val.as_bytes().last() {
        Some(b'm') | Some(b'M') => 1024 * 1024,
        Some(b'g') | Some(b'G') => 1024 * 1024 * 1024,
        Some(b't') | Some(b'T') => 1024 * 1024 * 1024 * 1024,
        _ => 1,
    };

    if mul != 1 {
        val.pop()
            .ok_or_else(|| anyhow!("Invalid value for {}", label))?;
    }

    let val: u64 = val
        .parse()
        .with_context(|| anyhow!("Invalid value for {}", label))?;

    Ok(val * mul)
}

fn parse_num(label: &str, val: Option<String>) -> Result<u32> {
    let Some(val) = val else {
        bail!("Missing {} value.", label);
    };

    val.parse()
        .with_context(|| anyhow!("Invalid value for {}", label))
}
