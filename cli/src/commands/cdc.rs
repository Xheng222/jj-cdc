

use clap::Subcommand;
use tracing::instrument;

use crate::{cli_util::CommandHelper, command_error::CommandError, ui::Ui};


#[derive(Subcommand, Clone, Debug)]
pub(crate) enum CdcCommand {
    Add(CdcSetArgs),
    Remove(CdcSetArgs),
    List(CdcListArgs),
}

/// Commands for managing file sets for change detection (CDC)
#[derive(clap::Args, Clone, Debug)]
pub struct CdcSetArgs {
    /// Path of the file(s) to include/exclude in the CDC set
    #[arg(value_name = "FILESETS", required = true)]
    paths: Vec<String>,
}

#[derive(clap::Args, Clone, Debug)]
pub struct CdcListArgs {
    // No arguments needed for listing CDC sets, 
    // but we can add options here in the future if needed
}

#[instrument(skip_all)]
pub(crate) async fn cmd_cdc(
    ui: &mut Ui,
    command: &CommandHelper,
    subcommand: &CdcCommand,
) -> Result<(), CommandError> {
    match subcommand {
        CdcCommand::Add(args) => cmd_cdc_add(ui, command, args).await,
        CdcCommand::Remove(args) => cmd_cdc_remove(ui, command, args).await,
        CdcCommand::List(args) => cmd_cdc_list(ui, command, args).await,
    }
}

async fn cmd_cdc_add(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcSetArgs,
) -> Result<(), CommandError> {
    writeln!(ui.status(), "Adding CDC set with paths: {:?}", args.paths)?;
    todo!()
}

async fn cmd_cdc_remove(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcSetArgs,
) -> Result<(), CommandError> {
    writeln!(ui.status(), "Removing CDC set with paths: {:?}", args.paths)?;
    todo!()
}

async fn cmd_cdc_list(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcListArgs,
) -> Result<(), CommandError> {
    writeln!(ui.status(), "Listing CDC sets...")?;
    todo!()
}







