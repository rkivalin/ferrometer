use regex::Regex;

use crate::error::Result;
use crate::signal::{Labels, Metric};

pub fn collect(filter: &Regex) -> Result<Vec<Metric>> {
    let devs = procfs::net::dev_status()
        .map_err(|e| crate::error::Error::Collector(format!("netdev: {e}")))?;
    let mut metrics = Vec::new();

    for (name, status) in &devs {
        if !filter.is_match(name) {
            continue;
        }

        let mut labels = Labels::new();
        labels.insert("device".into(), name.clone());

        let counters: &[(&str, u64)] = &[
            ("node_network_receive_bytes_total", status.recv_bytes),
            ("node_network_receive_packets_total", status.recv_packets),
            ("node_network_receive_errs_total", status.recv_errs),
            ("node_network_receive_drop_total", status.recv_drop),
            ("node_network_receive_fifo_total", status.recv_fifo),
            ("node_network_receive_frame_total", status.recv_frame),
            ("node_network_transmit_bytes_total", status.sent_bytes),
            ("node_network_transmit_packets_total", status.sent_packets),
            ("node_network_transmit_errs_total", status.sent_errs),
            ("node_network_transmit_drop_total", status.sent_drop),
            ("node_network_transmit_fifo_total", status.sent_fifo),
            ("node_network_transmit_colls_total", status.sent_colls),
            (
                "node_network_transmit_carrier_total",
                status.sent_carrier,
            ),
        ];

        for &(metric_name, value) in counters {
            metrics.push(Metric::counter(
                metric_name,
                value as f64,
                labels.clone(),
            ));
        }
    }

    Ok(metrics)
}
