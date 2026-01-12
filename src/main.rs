use std::env;

use anyhow::Result;

use crate::cli::Command;

#[macro_use]
mod log;

#[allow(unused, non_camel_case_types)]
mod bindings;
mod bindings_ext;
mod cli;
mod commands;
mod config;
mod io_buffers;
mod io_descriptor_map;
mod io_worker;
mod parts;
mod queue_limits;
mod runtime;
mod sqes;
mod task;
mod types;
mod util;

fn main() -> Result<()> {
    match cli::parse_cli(env::args())? {
        Command::Version(_) => commands::version::run(),
        Command::Help(_) => commands::help::run(),
        Command::Init(init) => commands::init::run(init),
        Command::Start(start) => commands::start::run(start),
        Command::Expand(expand) => commands::expand::run(expand),
    }
}
