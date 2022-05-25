use std::{
    fs::{create_dir, remove_file, rename, File},
    io::Read,
    mem::size_of,
    path::Path,
    ptr::slice_from_raw_parts,
    sync::Arc,
};

use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};

use super::page::{PageId, PAGE_SIZE};

pub const PAGE_PER_FILE: usize = 1024;

pub struct Persistencer {
    root_dir: String,
}

impl Persistencer {
    pub fn new(root_dir: &str) -> Self {
        create_dir(root_dir).unwrap();
        create_dir(format!("{}/store", root_dir)).unwrap();
        create_dir(format!("{}/log", root_dir)).unwrap();
        Self {
            root_dir: root_dir.to_string(),
        }
    }

    pub fn load(root_dir: &str) -> Self {
        let result = Self {
            root_dir: root_dir.to_string(),
        };
        assert!(Path::exists(Path::new(root_dir)));
        assert!(Path::exists(Path::new(&format!("{}/store", root_dir))));
        assert!(Path::exists(Path::new(&format!("{}/log", root_dir))));
        let log_dir1 = format!("{}/log/log_", root_dir);
        if Path::exists(Path::new(&log_dir1)) {
            remove_file(Path::new(&log_dir1)).unwrap();
        }
        let log_dir = format!("{}/log/log", root_dir);
        if Path::exists(Path::new(&log_dir)) {
            let mut log_file = File::options().read(true).open(&log_dir).unwrap();
            let mut buf = [0; 8];
            log_file.read_exact(&mut buf).unwrap();
            let len = u64::from_le_bytes(buf) as usize;
            let id_n_per_page = PAGE_SIZE.div_floor(size_of::<PageId>());
            let id_page_n =
                (len + size_of::<usize>().div_ceil(size_of::<PageId>())).div_ceil(id_n_per_page);
            let log_len = (id_page_n + len) * PAGE_SIZE;
            let mmap = unsafe { MmapOptions::new().len(log_len).map(&log_file) }.unwrap();
            let (slice1, slice2) = mmap.split_at(id_page_n * PAGE_SIZE);
            let (_, slice1) = slice1.split_at(size_of::<usize>());
            let (_preffix, page_ids, _suffix) = unsafe { slice1.align_to::<PageId>() };
            let (pages, _remainder) = slice2.as_chunks::<PAGE_SIZE>();
            let mut flushes = Vec::new();
            for idx in 0..len {
                flushes.push((page_ids[idx], unsafe {
                    &*slice_from_raw_parts::<u8>(pages[idx].as_ptr(), pages.len())
                }));
            }
            result.flush_inner(flushes);
            remove_file(log_dir).unwrap();
        }
        result
    }

    #[inline]
    fn get_file_idx(page_id: PageId) -> usize {
        page_id as usize / PAGE_PER_FILE
    }

    fn get_file(&self, file_idx: usize, init: bool) -> File {
        let file_dir = format!("{}/store/{}", self.root_dir, file_idx);
        let exist = Path::exists(Path::new(&file_dir));
        if !exist {
            assert!(init);
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(file_dir)
                .unwrap();
            file.set_len((PAGE_SIZE * PAGE_PER_FILE) as u64).unwrap();
            file
        } else {
            File::options()
                .read(true)
                .write(true)
                .open(file_dir)
                .unwrap()
        }
    }

    fn mmap_loc(&self, page_id: PageId, init: bool) -> (File, usize) {
        let file_idx = Self::get_file_idx(page_id);
        let file = self.get_file(file_idx, init);
        let off = (page_id as usize % PAGE_PER_FILE) * PAGE_SIZE;
        (file, off)
    }

    pub fn new_mmap(&self, page_id: PageId) -> MmapMut {
        let (file, off) = self.mmap_loc(page_id, true);
        unsafe {
            MmapOptions::new()
                .offset(off as u64)
                .len(PAGE_SIZE)
                .map_copy(&file)
                .unwrap()
        }
    }

    pub unsafe fn load_mmap(&self, page_id: PageId) -> MmapMut {
        let (file, off) = self.mmap_loc(page_id, false);
        MmapOptions::new()
            .offset(off as u64)
            .len(PAGE_SIZE)
            .map_copy(&file)
            .unwrap()
    }

    pub fn log(&self, logging: &DashMap<PageId, Arc<MmapMut>>) {
        let log_dir1 = format!("{}/log/log_", self.root_dir);
        let log_file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_dir1)
            .unwrap();
        let len = logging.len();
        let id_n_per_page = (PAGE_SIZE - size_of::<usize>()).div_floor(size_of::<PageId>());
        let id_page_n =
            (len + size_of::<usize>().div_ceil(size_of::<PageId>())).div_ceil(id_n_per_page);
        let log_len = (id_page_n + len) * PAGE_SIZE;
        log_file.set_len(log_len as u64).unwrap();
        let mut mmap = unsafe { MmapOptions::new().len(log_len).map_mut(&log_file) }.unwrap();
        let (slice1, slice2) = mmap.split_at_mut(id_page_n * PAGE_SIZE);
        let (slice3, slice1) = slice1.split_at_mut(size_of::<usize>());
        let (_preffix, page_ids, _suffix) = unsafe { slice1.align_to_mut::<PageId>() };
        slice3.copy_from_slice(&u64::to_le_bytes(len as u64));
        let (pages, _remainder) = slice2.as_chunks_mut::<PAGE_SIZE>();
        for (idx, e) in logging.iter().enumerate() {
            page_ids[idx] = *e.key();
            pages[idx].clone_from_slice(e.value());
        }
        mmap.flush_async().unwrap();
    }

    pub fn finish_log(&self) {
        let log_dir1 = format!("{}/log/log_", self.root_dir);
        let log_dir = format!("{}/log/log", self.root_dir);
        rename(log_dir1, &log_dir).unwrap();
    }

    pub fn flush(&self, flush_map: &DashMap<PageId, Arc<MmapMut>>) {
        let mut flushes = Vec::new();
        for e in flush_map.iter() {
            flushes.push((*e.key(), unsafe {
                &*(slice_from_raw_parts(e.value().as_ptr(), e.value().len()))
            }));
        }
        self.flush_inner(flushes);
    }

    fn flush_inner(&self, mut flushes: Vec<(PageId, &[u8])>) {
        let mut flush_set = Vec::new();

        let mut flush_sets = Vec::new();
        let mut cur_fid = None;
        flushes.sort_by(|a, b| a.0.cmp(&b.0));
        for (page_id, mmap) in flushes {
            let fid = Self::get_file_idx(page_id);
            if let Some(cid) = cur_fid {
                if cid != fid {
                    flush_sets.push((cid, flush_set));
                    flush_set = Vec::new();
                    cur_fid = Some(fid);
                }
            } else {
                cur_fid = Some(fid);
            }
            flush_set.push((page_id, mmap));
        }
        if let Some(cid) = cur_fid {
            flush_sets.push((cid, flush_set));
        }
        for (file_idx, flush_set) in flush_sets {
            let file = self.get_file(file_idx, false);
            for (page_id, content) in flush_set {
                let off = (page_id as usize % PAGE_PER_FILE) * PAGE_SIZE;
                let mut mmap = unsafe {
                    MmapOptions::new()
                        .offset(off as u64)
                        .len(PAGE_SIZE)
                        .map_mut(&file)
                        .unwrap()
                };
                mmap.clone_from_slice(content);
                mmap.flush_async().unwrap();
            }
        }
        let log_dir = format!("{}/log/log", self.root_dir);
        remove_file(&log_dir).unwrap();
    }
}
