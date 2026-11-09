mod cli;
mod runner;

use clap::Parser;
use cli::{Cli, Config, RunnerError};
use diagweave::Report;
use runner::Runner;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run_app(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            let code = report
                .error_code()
                .and_then(|code| i32::try_from(code).ok())
                .or_else(|| report.inner().exit_code())
                .unwrap_or(1);
            eprintln!("{}", report.compact());
            ExitCode::from(code as u8)
        }
    }
}

fn run_app(cli: Cli) -> Result<(), Report<RunnerError>> {
    let config = Config::try_from(cli)?;
    Runner::new(config)?.run()
}
