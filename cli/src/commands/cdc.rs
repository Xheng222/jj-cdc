use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use clap::Subcommand;
use jj_lib::backend::FileId;
use jj_lib::backend::TreeValue;
use jj_lib::cdc::backend_wrapper::CdcBackendWrapper;
use jj_lib::file_util::BlockingAsyncReader;
use jj_lib::local_working_copy::TreeStateSettings;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::store::Store;
use tracing::instrument;

use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::user_error;
use crate::ui::Ui;

#[derive(Subcommand, Clone, Debug)]
pub(crate) enum CdcCommand {
    /// Add files to the CDC set, converting them from git storage to CDC
    /// storage
    Add(CdcSetArgs),
    /// Remove files from the CDC set, converting them from CDC storage back to
    /// git storage
    Remove(CdcSetArgs),
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
    }
}

async fn convert_file_storage(
    path: &RepoPathBuf,
    ws_path: &Path,
    store: &Arc<Store>,
    tree_state_settings: &TreeStateSettings,
    to_cdc: bool,
) -> Result<FileId, CommandError> {
    let disk_path = path.to_fs_path(ws_path).unwrap();
    let file = File::open(disk_path)?;

    let new_id = if to_cdc {
        let cdc_backend = store
            .backend_impl::<CdcBackendWrapper>()
            .ok_or_else(|| user_error("CDC backend is not enabled for this repository"))?;
        let pointer_content = cdc_backend
            .write_file_to_cdc(file)
            .await
            .map_err(|e| user_error(e))?;
        store
            .write_file(path, &mut std::io::Cursor::new(pointer_content))
            .await?
    } else {
        let mut contents = tree_state_settings
            .convert_eol_for_snapshot(BlockingAsyncReader::new(file))
            .await?;
        store.write_file(&path, &mut contents).await?
    };

    Ok(new_id)
}

async fn cmd_cdc_set(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcSetArgs,
    to_cdc: bool,
) -> Result<(), CommandError> {
    let mut workspace_command = command.workspace_helper(ui)?;

    let tree_state_settings =
        TreeStateSettings::try_from_user_settings(workspace_command.settings())?;
    let fileset = workspace_command.parse_file_patterns(ui, &args.paths)?;

    let mut tx = workspace_command.start_transaction();
    let ws_path = tx.base_workspace_helper().workspace_root();
    let wc_tree = tx.base_workspace_helper().working_copy().tree()?;
    let wc_commit_id = tx
        .base_workspace_helper()
        .get_wc_commit_id()
        .ok_or_else(|| user_error("This command must be run in a workspace"))?;

    let store = tx.repo().store();
    let wc_commit = store.get_commit(wc_commit_id)?;
    let cdc_backend = store
        .backend_impl::<CdcBackendWrapper>()
        .ok_or_else(|| user_error("CDC backend is not enabled for this repository"))?;

    let mut tree_builder = MergedTreeBuilder::new(wc_tree.clone());
    let mut changed = 0usize;

    for (path, value_result) in wc_tree.entries_matching(&fileset.to_matcher()) {
        let value = value_result?;
        let Some(Some(TreeValue::File {
            id,
            executable,
            copy_id,
        })) = value.as_resolved()
        else {
            writeln!(
                ui.warning_default(),
                "Skipping non-file or conflicted entry: {}",
                tx.base_workspace_helper().format_file_path(&path)
            )?;
            continue;
        };

        if cdc_backend.is_stored_as_cdc(id).map_err(|e| {
            user_error(format!(
                "Failed to check if file {} is stored as CDC: {}",
                tx.base_workspace_helper().format_file_path(&path),
                e
            ))
        })? == to_cdc
        {
            continue;
        }

        let new_id =
            convert_file_storage(&path, ws_path, store, &tree_state_settings, to_cdc).await?;

        tree_builder.set_or_remove(
            path,
            Merge::normal(TreeValue::File {
                id: new_id,
                executable: *executable,
                copy_id: copy_id.clone(),
            }),
        );
        changed += 1;
    }

    if changed == 0 {
        writeln!(ui.status(), "No files needed to change.")?;
        return Ok(());
    }

    let new_tree = tree_builder.write_tree().await?;
    tx.repo_mut()
        .rewrite_commit(&wc_commit)
        .set_tree(new_tree)
        .write()
        .await?;

    if to_cdc {
        writeln!(ui.status(), "{} files changed from git to cdc", changed)?;
        tx.finish(ui, format!("cdc add: {}", args.paths.join(" ")))
            .await?;
    } else {
        writeln!(ui.status(), "{} files changed from cdc to git", changed)?;
        tx.finish(ui, format!("cdc remove: {}", args.paths.join(" ")))
            .await?;
    }
    Ok(())
}

async fn cmd_cdc_add(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcSetArgs,
) -> Result<(), CommandError> {
    cmd_cdc_set(ui, command, args, true).await
}

async fn cmd_cdc_remove(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &CdcSetArgs,
) -> Result<(), CommandError> {
    cmd_cdc_set(ui, command, args, false).await
}
