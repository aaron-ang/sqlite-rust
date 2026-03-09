use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::Result;

/// Simple in-memory page cache keyed by SQLite page number.
///
/// This is intentionally minimal: fixed capacity, LRU eviction, and stores
/// pages as shared `Arc<[u8]>` so callers can clone cheaply without copying
/// the underlying bytes.
#[derive(Debug)]
pub struct PageCache {
    page_size: u32,
    max_pages: usize,
    map: HashMap<u32, Arc<[u8]>>,
    lru: VecDeque<u32>,
}

impl PageCache {
    pub fn new(page_size: u32, max_pages: usize) -> Self {
        Self {
            page_size,
            max_pages: max_pages.max(1),
            map: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Fetch a page, loading it via `loader` on miss.
    ///
    /// The returned buffer is an `Arc<[u8]>`. Cloning it is effectively
    /// zero-copy; all clones share the same underlying bytes.
    pub fn get_or_load<F>(&mut self, page_no: u32, mut loader: F) -> Result<Arc<[u8]>>
    where
        F: FnMut(u32) -> Result<Vec<u8>>,
    {
        if let Some(page) = self.map.get(&page_no) {
            let arc = Arc::clone(page);
            self.touch(page_no);
            return Ok(arc);
        }

        let page = loader(page_no)?;
        // Keep a defensive invariant about page size; if it doesn't match,
        // still cache what we got, but don't assume anything stronger here.
        let page = if page.len() < self.page_size as usize {
            // Zero-extend short pages to the configured page size.
            let mut padded = page;
            padded.resize(self.page_size as usize, 0u8);
            padded
        } else {
            page
        };

        self.insert_and_get(page_no, page)
    }

    fn insert_and_get(&mut self, page_no: u32, page: Vec<u8>) -> Result<Arc<[u8]>> {
        self.evict_if_needed();
        self.lru.retain(|&p| p != page_no);
        self.lru.push_front(page_no);
        let arc: Arc<[u8]> = Arc::from(page.into_boxed_slice());
        self.map.insert(page_no, Arc::clone(&arc));
        Ok(arc)
    }

    fn touch(&mut self, page_no: u32) {
        if let Some(pos) = self.lru.iter().position(|&p| p == page_no) {
            self.lru.remove(pos);
            self.lru.push_front(page_no);
        }
    }

    fn evict_if_needed(&mut self) {
        if self.map.len() < self.max_pages {
            return;
        }
        if let Some(oldest) = self.lru.pop_back() {
            self.map.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PageCache;

    #[test]
    fn caches_and_evicts_pages() {
        let mut cache = PageCache::new(5, 2);

        let mut loads = 0usize;
        let mut loader = |p: u32| {
            loads += 1;
            Ok(vec![p as u8; 5])
        };

        // First access loads.
        let p1 = cache.get_or_load(1, &mut loader).unwrap();
        assert_eq!(p1.as_ref(), &[1u8; 5]);

        // Second access to same page is cached (same contents).
        let p1_again = cache.get_or_load(1, &mut loader).unwrap();
        assert_eq!(p1_again.as_ref(), &[1u8; 5]);

        // Fill cache with another page.
        let _p2 = cache.get_or_load(2, &mut loader).unwrap();

        // Access a third page, causing eviction of LRU (page 1).
        let _p3 = cache.get_or_load(3, &mut loader).unwrap();

        // Page 1 should be loaded again after eviction with a fresh loader.
        let mut loader2 = |p: u32| {
            loads += 1;
            Ok(vec![p as u8; 5])
        };
        let _p1_third = cache.get_or_load(1, &mut loader2).unwrap();
        // We should have performed four loads in total: three with loader and one with loader2.
        assert_eq!(loads, 4);
    }
}
