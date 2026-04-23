use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

pub type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: Arc<str>,
    pub labels: Arc<Labels>,
    pub value: f64,
    pub timestamp: SystemTime,
    pub metric_type: MetricType,
}

impl Metric {
    pub fn gauge(name: &str, value: f64, labels: Arc<Labels>) -> Self {
        Self {
            name: intern_name(name),
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Gauge,
        }
    }

    pub fn counter(name: &str, value: f64, labels: Arc<Labels>) -> Self {
        Self {
            name: intern_name(name),
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Counter,
        }
    }
}

/// Process-wide intern pool for metric names. Most workloads have a small
/// fixed vocabulary (a few hundred names across all collectors), so one
/// allocation per unique name amortizes to zero per-metric cost. The
/// interner is a HashSet<Arc<str>> so lookups by &str work via Arc's
/// Borrow<str> impl.
static NAME_INTERNER: LazyLock<Mutex<HashSet<Arc<str>>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub fn intern_name(name: &str) -> Arc<str> {
    let mut set = NAME_INTERNER.lock().expect("name interner poisoned");
    if let Some(existing) = set.get(name) {
        return existing.clone();
    }
    let arc: Arc<str> = Arc::from(name);
    set.insert(arc.clone());
    arc
}
