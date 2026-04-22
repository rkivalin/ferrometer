fn main() {
    #[cfg(feature = "forwarder-otlphttp")]
    {
        prost_build::Config::new()
            .btree_map(["."])
            .compile_protos(
                &["proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto"],
                &["proto/"],
            )
            .expect("failed to compile OTLP proto files");
    }
}
