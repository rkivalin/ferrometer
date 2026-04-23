use std::collections::HashSet;
use std::sync::Arc;

use crate::signal::Labels;

/// Intern cache for label sets. Collectors build a raw `Labels` each tick;
/// `intern` folds in the base labels (typically instance + configured static
/// labels), deduplicates against previously seen sets, and returns a shared
/// `Arc<Labels>`. Since label shape is usually stable across ticks, the
/// cache grows once and is reused indefinitely.
///
/// On conflict between caller-supplied labels and base labels, the base
/// wins — the intent is that base labels are authoritative identifiers
/// (instance, job) that should never be overridden by whatever was scraped.
pub struct LabelCache {
    base: Labels,
    cache: HashSet<Arc<Labels>>,
}

impl LabelCache {
    pub fn new(base: Labels) -> Self {
        Self {
            base,
            cache: HashSet::new(),
        }
    }

    pub fn intern(&mut self, mut labels: Labels) -> Arc<Labels> {
        for (k, v) in &self.base {
            labels.insert(k.clone(), v.clone());
        }
        if let Some(arc) = self.cache.get(&labels) {
            return arc.clone();
        }
        let arc = Arc::new(labels);
        self.cache.insert(arc.clone());
        arc
    }
}
