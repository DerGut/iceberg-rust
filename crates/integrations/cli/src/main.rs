// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::MaxRows;
use iceberg_cli::{ICEBERG_CLI_VERSION, IcebergCatalogList, exec, namespaces};

// TODO: these should maybe be defined is a separate module so that different
// command execution modules can import them.
#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'r',
        long,
        help = "Parse catalog config instead of using ~/.icebergrc"
    )]
    rc: Option<String>,

    #[clap(short = 'c', long = "catalog", help = "Use a specific catalog")]
    catalog: Option<String>,

    #[clap(long, value_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, PartialEq)]
enum Command {
    // Interact with namespaces
    Namespaces {
        #[command(subcommand)]
        subcommand: Option<namespaces::Command>,
    },
    Exec,
}

#[tokio::main]
/// Calls [`main_inner`], then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    if let Err(e) = main_inner().await {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

async fn main_inner() -> anyhow::Result<()> {
    let args = Args::parse();

    if !args.quiet {
        println!("ICEBERG CLI v{}", ICEBERG_CLI_VERSION);
    }

    let rc = match args.rc {
        Some(file) => PathBuf::from_str(&file)?,
        None => dirs::home_dir()
            .map(|h| h.join(".icebergrc"))
            .ok_or_else(|| anyhow::anyhow!("cannot find home directory"))?,
    };

    let catalogs = IcebergCatalogList::parse(&rc).await?;

    let selected_catalog = args
        .catalog
        .map(|name| {
            catalogs
                .catalog(&name)
                .ok_or(anyhow::anyhow!("catalog {} not found", name))
        })
        .transpose()?;

    match &args.command {
        Some(Command::Namespaces { subcommand }) => {
            if let Some(catalog) = selected_catalog {
                namespaces::exec(catalog, subcommand).await
            } else {
                Err(anyhow::anyhow!("no catalog selected"))
            }
        }
        Some(Command::Exec) | None => {
            exec::exec(&catalogs, args.format, args.quiet, args.maxrows, args.color).await
        }
    }
}
