// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cmp::Ordering,
    collections::{btree_map, hash_map, BTreeMap, BTreeSet, HashMap, VecDeque},
    io,
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use prost::Message;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};

use super::{iterator::ManifestIter, manifest, proto::VersionEditBuilder, table::format::Key};
use crate::{
    error::{Error, Result},
    lsm::proto::{version_edit, VersionEdit},
};

// TODO(luhuanbing): 7 make sense?
const NUM_LEVELS: usize = 7;

#[derive(Clone, Default)]
pub struct FileMetadata {
    pub name: String,
    pub bucket: String,
    pub tenant: String,
    pub level: u32,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
    pub file_size: u64,
}

#[derive(Clone, Default)]
pub struct LevelFiles<T>
where
    T: Ord + Clone + PartialOrd + PartialEq + Deref + From<FileMetadata>,
{
    pub files: BTreeSet<T>, // ordered FileMetadata
}

impl LevelFiles<OrdByUpperBound> {
    pub fn iter(&self) -> ManifestIter {
        ManifestIter::new(self.files.clone())
    }
}

#[derive(Clone)]
pub struct BucketVersion {
    pub l0_level: Vec<FileMetadata>,
    pub non_l0_levels: Vec<LevelFiles<OrdByUpperBound>>,
    pub files: HashMap<String, FileMetadata>, // filename -> file (for delete)
}

impl Default for BucketVersion {
    fn default() -> Self {
        let non_l0_levels = (1..NUM_LEVELS)
            .into_iter()
            .map(|_| LevelFiles::default())
            .collect();
        Self {
            non_l0_levels,
            files: HashMap::new(),
            l0_level: Vec::new(),
        }
    }
}

#[derive(Default, Clone)]
pub struct Version {
    pub inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
pub struct Inner {
    pub buckets: BTreeMap<String, BucketVersion>, // bucket => levels;
}

impl Version {
    pub async fn apply(&mut self, ve: &VersionEdit) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.apply(ve).await?;
        Ok(())
    }

    pub async fn generate_snapshot(&self, next_file_num: u64) -> VersionEdit {
        let inner = self.inner.lock().await;
        inner.generate_snapshot(next_file_num).await
    }

    pub async fn bucket_version(&self, bucket: &str) -> Result<BucketVersion> {
        let inner = self.inner.lock().await;
        Ok(inner
            .buckets
            .get(bucket)
            .ok_or_else(|| Error::NotFound(format!("bucket {}", bucket)))?
            .to_owned())
    }
}

impl Inner {
    async fn apply(&mut self, ve: &VersionEdit) -> Result<()> {
        for add_bucket in &ve.add_buckets {
            match self.buckets.entry(add_bucket.name.to_owned()) {
                btree_map::Entry::Vacant(ent) => Some(ent.insert(BucketVersion::default())),
                btree_map::Entry::Occupied(_) => None,
            };
        }
        for bucket in &ve.remove_buckets {
            self.buckets.remove(bucket);
        }

        for add_file in &ve.add_files {
            if let Some(bucket) = self.buckets.get_mut(&add_file.bucket) {
                let file_meta = FileMetadata {
                    name: add_file.name.to_owned(),
                    bucket: add_file.bucket.to_owned(),
                    tenant: add_file.tenant.to_owned(),
                    level: add_file.level,
                    lower_bound: add_file.lower_bound.to_owned(),
                    upper_bound: add_file.upper_bound.to_owned(),
                    file_size: add_file.file_size.to_owned(),
                };

                match bucket.files.entry(add_file.name.to_owned()) {
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(file_meta.to_owned());
                    }
                    hash_map::Entry::Occupied(_) => {
                        return Err(Error::AlreadyExists(format!(
                            "bucket {}, file {}",
                            &add_file.bucket, &add_file.name
                        )))
                    }
                };

                let has_dup = if add_file.level == 0 {
                    bucket.l0_level.push(file_meta);
                    false
                } else {
                    let level_files = bucket
                        .non_l0_levels
                        .get_mut((add_file.level - 1) as usize)
                        .ok_or_else(|| Error::Internal("current version not found".to_string()))?;
                    !level_files.files.insert(OrdByUpperBound(file_meta))
                };
                if has_dup {
                    return Err(Error::AlreadyExists(format!(
                        "bucket {}, level {}, key {:?}",
                        &add_file.bucket, &add_file.level, &add_file.lower_bound
                    )));
                }
            } else {
                return Err(Error::NotFound(format!("bucket {}", &add_file.bucket)));
            }
        }
        for file in &ve.remove_files {
            if let Some(bucket) = self.buckets.get_mut(&file.bucket) {
                let removed = bucket.files.remove(&file.name);
                if let Some(f) = removed {
                    if f.level == 0 {
                        bucket.l0_level.retain(|e| e.lower_bound != f.lower_bound)
                    } else {
                        bucket.non_l0_levels[(f.level - 1) as usize].files.remove(
                            &OrdByUpperBound(FileMetadata {
                                lower_bound: f.lower_bound,
                                ..Default::default()
                            }),
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn generate_snapshot(&self, next_file_num: u64) -> VersionEdit {
        let mut b = VersionEditBuilder::default();
        let mut buckets = Vec::new();
        for (name, bucket) in &self.buckets {
            buckets.push(version_edit::Bucket {
                name: name.to_owned(),
            });
            for file in bucket.files.values() {
                b.add_files(vec![version_edit::File {
                    range_id: 1,
                    bucket: file.bucket.to_owned(),
                    tenant: file.tenant.to_owned(),
                    name: file.name.to_owned(),
                    level: file.level,
                    lower_bound: file.lower_bound.to_owned(),
                    upper_bound: file.upper_bound.to_owned(),
                    file_size: file.file_size.to_owned(),
                }]);
            }
        }
        b.set_next_file_num(next_file_num);
        b.build()
    }
}

#[derive(Clone, Default)]
pub struct OrdByUpperBound(pub FileMetadata);

impl From<FileMetadata> for OrdByUpperBound {
    fn from(m: FileMetadata) -> Self {
        Self(m)
    }
}

impl Deref for OrdByUpperBound {
    type Target = FileMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ord for OrdByUpperBound {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Key::from(self.0.upper_bound.as_slice()).cmp(&Key::from(other.0.upper_bound.as_slice()))
    }
}

impl PartialOrd for OrdByUpperBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdByUpperBound {
    fn eq(&self, other: &Self) -> bool {
        self.0.lower_bound == other.0.lower_bound
    }
}

impl Eq for OrdByUpperBound {}

#[derive(Clone)]
pub struct VersionSet {
    inner: Arc<Mutex<VersionSetInner>>,
}

struct VersionSetInner {
    path: PathBuf,
    versions: VecDeque<Version>,
    manifest: Option<manifest::Writer>,
    next_file_num: u64,
    current_file_num: u64,
}

impl VersionSet {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut inner = VersionSetInner {
            path: path.into(),
            versions: VecDeque::new(),
            manifest: None,
            next_file_num: 0,
            current_file_num: 0,
        };
        match inner.find_current_manifest().await? {
            Some(last_file_num) => {
                inner.load(last_file_num).await?;
            }
            None => {
                inner.create().await?;
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl VersionSet {
    pub async fn current_version(&self) -> Version {
        let inner = self.inner.lock().await;
        inner.current_version()
    }

    #[allow(dead_code)]
    pub async fn log_and_apply(&self, ve: VersionEdit) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let rolleded = if match &inner.manifest {
            Some(manifest) => manifest.accumulated_size().await > manifest::MAX_FILE_SIZE,
            None => true, // new or restarted
        } {
            let new_file_num = inner.get_next_file_num();
            let next_file_num = inner.next_file_num;
            inner.create_manifest(new_file_num, next_file_num).await?;
            inner.current_file_num = new_file_num;
            true
        } else {
            false
        };

        let mut manifest = inner.manifest.take().unwrap();
        manifest.append(&ve.encode_to_vec()).await?;
        manifest.flush_and_sync(rolleded).await?;
        inner.manifest = Some(manifest);
        if rolleded {
            inner.update_current(inner.current_file_num).await?;
        }

        let mut new_version = inner.current_version().to_owned();
        new_version.apply(&ve).await?;

        inner.versions.push_back(new_version);

        Ok(())
    }

    pub async fn get_next_file_num(&self, count: u64) -> Result<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut inner = self.inner.lock().await;
        let mut res = Vec::new();
        for _ in 0..count {
            res.push(inner.get_next_file_num());
        }
        let ve = VersionEditBuilder::default()
            .set_next_file_num(inner.next_file_num)
            .build();
        let mut new_version = inner.current_version().to_owned();
        new_version.apply(&ve).await?;
        inner.versions.push_back(new_version);
        Ok(res)
    }
}

impl VersionSetInner {
    fn get_next_file_num(&mut self) -> u64 {
        let num = self.next_file_num;
        self.next_file_num += 1;
        num
    }

    fn current_version(&self) -> Version {
        self.versions.back().unwrap().to_owned()
    }

    async fn create_manifest(&mut self, create_file_num: u64, next_file_num: u64) -> Result<()> {
        let file = {
            let filename = self.file_path(create_file_num);
            fs::create_dir_all(filename.parent().unwrap()).await?;
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(filename)
                .await?
        };
        let mut writer = manifest::Writer::new(file);

        let snapshot = self
            .current_version()
            .generate_snapshot(next_file_num)
            .await;

        writer.append(&snapshot.encode_to_vec()).await?;

        self.manifest = Some(writer);
        Ok(())
    }

    fn file_path(&self, manifest_file_name: u64) -> PathBuf {
        self.path
            .join(format!("MANIFEST-{:0>6}", manifest_file_name))
    }

    async fn create(&mut self) -> Result<()> {
        let new_version = Version::default();
        self.versions.push_back(new_version);
        self.current_file_num = self.get_next_file_num();
        let next_file_num = self.next_file_num;
        self.create_manifest(self.current_file_num, next_file_num)
            .await?;
        let mut manifest = self.manifest.take().unwrap();
        manifest.flush_and_sync(true).await?;
        self.manifest = Some(manifest);
        self.update_current(self.current_file_num).await?;
        Ok(())
    }

    async fn load(&mut self, manifest_file_num: u64) -> Result<()> {
        self.current_file_num = manifest_file_num;
        let manifest_file = {
            let filename = self.file_path(self.current_file_num);
            fs::create_dir_all(filename.parent().unwrap()).await?;
            fs::OpenOptions::new()
                .create(false)
                .append(true)
                .read(true)
                .open(filename)
                .await?
        };

        let mut new_version = Version::default();
        let mut r = manifest::Reader::new(manifest_file);
        loop {
            let res = r.read().await;
            if let Err(Error::IO(err)) = &res {
                if Self::is_eof(err) {
                    break;
                }
            }
            let chunk_data = res?;
            let ve = VersionEdit::decode(chunk_data.as_slice()).unwrap();
            if ve.next_file_num > 0 {
                self.next_file_num = ve.next_file_num;
            }
            new_version.apply(&ve).await?;
        }
        self.versions.push_back(new_version);
        Ok(())
    }

    fn is_eof(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::UnexpectedEof
    }

    async fn find_current_manifest(&self) -> Result<Option<u64>> {
        let path = self.path.join("CURRENT");
        let res = fs::read_to_string(&path).await;
        if let Err(io_err) = &res {
            if io_err.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
        }
        let content = res?;
        if !content.ends_with('\n') {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let manifest_name = content.trim().split_once('-');
        if manifest_name.is_none() {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let (prefix, num) = manifest_name.unwrap();
        if prefix != "MANIFEST" {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let num = num.parse::<u64>();
        if num.is_err() {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        Ok(Some(num.unwrap()))
    }

    async fn update_current(&self, file_num: u64) -> Result<()> {
        let tmp_path = self.path.join(format!("CURRENT.{}.dbtmp", file_num));
        let curr_path = self.path.join("CURRENT");
        let _ = fs::remove_file(&tmp_path).await;
        {
            let mut tmp_w = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)
                .await?;
            tmp_w
                .write_all(format!("MANIFEST-{:0>6}\n", file_num).as_bytes())
                .await?;
            tmp_w.sync_all().await?;
        }
        fs::rename(tmp_path, curr_path).await?;
        Ok(())
    }
}
