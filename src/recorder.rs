use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};

/// Metric recorder
pub struct DataDogRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl DataDogRecorder {
    pub(crate) fn new(registry: Arc<Registry<Key, AtomicStorage>>) -> Self {
        DataDogRecorder { registry }
    }
}

impl Recorder for DataDogRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        unimplemented!()
    }

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        unimplemented!()
    }

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        unimplemented!()
    }

    fn register_counter(&self, key: &Key, _: &Metadata) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key, _: &Metadata) -> Gauge {
        self.registry.get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key, _: &Metadata) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}
