use std::collections::BTreeMap;
use std::time::SystemTime;

pub type Labels = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub labels: Labels,
    pub value: f64,
    pub timestamp: SystemTime,
    pub metric_type: MetricType,
}

impl Metric {
    pub fn gauge(name: impl Into<String>, value: f64, labels: Labels) -> Self {
        Self {
            name: name.into(),
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Gauge,
        }
    }

    pub fn counter(name: impl Into<String>, value: f64, labels: Labels) -> Self {
        Self {
            name: name.into(),
            labels,
            value,
            timestamp: SystemTime::now(),
            metric_type: MetricType::Counter,
        }
    }
}
