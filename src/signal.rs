use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

pub type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: &'static str,
    pub labels: Arc<Labels>,
    pub value: f64,
    pub timestamp: SystemTime,
    pub metric_type: MetricType,
}

impl Metric {
    pub fn gauge(name: &'static str, value: f64, labels: Arc<Labels>) -> Self {
        Self {
            name,
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Gauge,
        }
    }

    pub fn counter(name: &'static str, value: f64, labels: Arc<Labels>) -> Self {
        Self {
            name,
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Counter,
        }
    }
}
