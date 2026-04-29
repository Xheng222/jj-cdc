#![expect(missing_docs)]

use std::{
    fs::File,
    path::{Path, PathBuf},
    pin::Pin,
    time::SystemTime,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use gix::objs::FindHeader;
use once_cell::sync::OnceCell;
use tokio::io::AsyncRead;

use crate::{
    backend::{
        Backend, BackendError, BackendResult, ChangeId, Commit, CommitId, CopyHistory, CopyId, CopyRecord, FileId, RelatedCopy, SigningFn, SymlinkId, Tree, TreeId
    }, cdc::{
        cdc_config::CDC_POINTER_SIZE,
        cdc_error::{CdcError, CdcResult},
        pointer::CdcPointer,
        store_backend::{CdcStoreBackend, ChunkStoreBackend},
    }, git_backend::{GitBackend, GitBackendLoadError}, index::Index, object_id::ObjectId, repo_path::{RepoPath, RepoPathBuf}, settings::UserSettings
};

/// CDC backend wrapper
pub struct CdcBackendWrapper {
    /// git backend
    git_backend: GitBackend,
    /// cdc backend
    cdc_path: PathBuf,
    cdc_backend: OnceCell<Box<dyn CdcStoreBackend>>,
}

impl CdcBackendWrapper {
    pub fn name() -> &'static str {
        GitBackend::name()
    }

    pub fn load_at_workspace(
        settings: &UserSettings,
        store_path: &Path,
        workspace_root: Option<&Path>,
    ) -> Result<Self, Box<GitBackendLoadError>> {
        let git_backend = GitBackend::load_at_workspace(settings, store_path, workspace_root)?;
        let cdc_path = store_path.join("cdc");

        Ok(Self {
            git_backend: git_backend,
            cdc_path: cdc_path,
            cdc_backend: OnceCell::new(),
        })
    }

    pub fn inner(&self) -> &GitBackend {
        &self.git_backend
    }

    fn get_store_backend(&self) -> CdcResult<&Box<dyn CdcStoreBackend>> {
        self.cdc_backend.get_or_try_init(|| {
            let cdc_backend = ChunkStoreBackend::new(&self.cdc_path)?;
            Ok(Box::new(cdc_backend))
        })
    }

    pub async fn write_file_to_cdc(&self, file: File) -> CdcResult<Vec<u8>> {
        self.get_store_backend()?.write_file(file).await
    }

    pub async fn read_file_from_cdc(
        &self,
        pointer_content: &CdcPointer,
        file: &mut File,
    ) -> CdcResult<usize> {
        self.get_store_backend()?
            .read_file(pointer_content, file)
            .await
    }

    pub fn is_stored_as_cdc(&self, id: &FileId) -> CdcResult<bool> {
        let repo = self.git_backend.git_repo();
        let oid = gix::ObjectId::from_bytes_or_panic(id.as_bytes());
        let header = repo.try_find_header(oid).map_err(CdcError::from_git)?;
        if let Some(header) = header {
            if header.kind() != gix::objs::Kind::Blob {
                return Ok(false);
            }

            if header.size() != CDC_POINTER_SIZE {
                return Ok(false);
            }

            let object = repo.find_blob(oid).map_err(CdcError::from_git)?;
            if CdcPointer::try_parse_from_bytes(&object.data).is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[async_trait]
impl Backend for CdcBackendWrapper {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        self.git_backend.commit_id_length()
    }

    fn change_id_length(&self) -> usize {
        self.git_backend.change_id_length()
    }

    fn root_commit_id(&self) -> &CommitId {
        self.git_backend.root_commit_id()
    }

    fn root_change_id(&self) -> &ChangeId {
        self.git_backend.root_change_id()
    }

    fn empty_tree_id(&self) -> &TreeId {
        self.git_backend.empty_tree_id()
    }

    fn concurrency(&self) -> usize {
        self.git_backend.concurrency()
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        self.git_backend.read_file(path, id).await
    }

    async fn write_file(
        &self,
        path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        self.git_backend.write_file(path, contents).await
    }

    async fn read_symlink(&self, path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        self.git_backend.read_symlink(path, id).await
    }

    async fn write_symlink(&self, path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        self.git_backend.write_symlink(path, target).await
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        self.git_backend.read_copy(id).await
    }

    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        self.git_backend.write_copy(copy).await
    }

    async fn get_related_copies(&self, copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        self.git_backend.get_related_copies(copy_id).await
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        self.git_backend.read_tree(path, id).await
    }

    async fn write_tree(&self, path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        self.git_backend.write_tree(path, contents).await
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        self.git_backend.read_commit(id).await
    }

    async fn write_commit(
        &self,
        contents: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        self.git_backend.write_commit(contents, sign_with).await
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        self.git_backend.get_copy_records(paths, root, head)
    }

    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        self.git_backend.gc(index, keep_newer)?;
        let jj_repo = match gix::open(&self.git_backend.git_repo_path()) {
            Ok(repo) => repo,
            Err(e) => return Err(BackendError::Other(e.into())),
        };

        let mut keep_manifests = Vec::new();
        if let Ok(objects) = jj_repo.objects.iter() {
            for object in objects {
                match object {
                    Ok(object) => match jj_repo.objects.try_header(&object) {
                        Ok(Some(header)) => {
                            if header.kind != gix::objs::Kind::Blob {
                                continue;
                            }

                            if header.size != CDC_POINTER_SIZE {
                                continue;
                            }

                            match jj_repo.find_blob(object) {
                                Ok(object) => {
                                    if let Some(pointer) =
                                        CdcPointer::try_parse_from_bytes(&object.data)
                                    {
                                        keep_manifests.push(pointer);
                                    }
                                }
                                Err(e) => return Err(BackendError::Other(e.into())),
                            }
                        }
                        Ok(None) => continue,
                        Err(e) => return Err(BackendError::Other(e.into())),
                    },
                    Err(e) => return Err(BackendError::Other(e.into())),
                }
            }
        }

        self.get_store_backend()?.gc(keep_manifests)?;
        Ok(())
    }
}

impl std::fmt::Debug for CdcBackendWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcBackendWrapper")
            .field("inner", &self.git_backend)
            .finish()
    }
}
