use std::collections::HashSet;
use std::sync::Arc;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Mutex,
    time::SystemTime,
};

use crate::attributes::AttributeSet;
use crate::metrics::data::{self, Aggregation, DataPoint, Temporality};
use crate::metrics::internal::aggregate::BoundedMeasure;
use opentelemetry::{global, metrics::MetricsError};

use super::{
    aggregate::{is_under_cardinality_limit, STREAM_OVERFLOW_ATTRIBUTE_SET},
    AtomicTracker, Number,
};

/// The storage for sums.
#[derive(Default)]
struct ValueMap<T: Number<T>> {
    values: Mutex<HashMap<AttributeSet, T>>,
    bound_values: Mutex<HashMap<AttributeSet, Arc<dyn AtomicTracker<T> + Send + Sync + 'static>>>,
}

impl<T: Number<T>> ValueMap<T> {
    fn new() -> Self {
        ValueMap {
            values: Mutex::new(HashMap::new()),
            bound_values: Mutex::new(HashMap::new()),
        }
    }
}

impl<T: Number<T>> ValueMap<T> {
    fn measure(&self, measurement: T, attrs: AttributeSet) {
        if let Ok(mut values) = self.values.lock() {
            let size = values.len();
            match values.entry(attrs) {
                Entry::Occupied(mut occupied_entry) => {
                    let sum = occupied_entry.get_mut();
                    *sum += measurement;
                }
                Entry::Vacant(vacant_entry) => {
                    if is_under_cardinality_limit(size) {
                        vacant_entry.insert(measurement);
                    } else {
                        values
                            .entry(STREAM_OVERFLOW_ATTRIBUTE_SET.clone())
                            .and_modify(|val| *val += measurement)
                            .or_insert(measurement);
                        global::handle_error(MetricsError::Other("Warning: Maximum data points for metric stream exceeded. Entry added to overflow.".into()));
                    }
                }
            }
        }
    }

    fn get_atomic_tracker(&self, attrs: AttributeSet) -> Arc<dyn AtomicTracker<T>> {
        let mut values = self
            .bound_values
            .lock()
            .expect("Sum atomic tracker mutex is poisoned");
        let entry = values
            .entry(attrs)
            .or_insert_with(|| Arc::new(T::new_atomic_tracker()));

        entry.clone()
    }
}

/// Summarizes a set of measurements made as their arithmetic sum.
pub(crate) struct Sum<T: Number<T>> {
    value_map: ValueMap<T>,
    monotonic: bool,
    start: Mutex<SystemTime>,
}

impl<T: Number<T>> Sum<T> {
    /// Returns an aggregator that summarizes a set of measurements as their
    /// arithmetic sum.
    ///
    /// Each sum is scoped by attributes and the aggregation cycle the measurements
    /// were made in.
    pub(crate) fn new(monotonic: bool) -> Self {
        Sum {
            value_map: ValueMap::new(),
            monotonic,
            start: Mutex::new(SystemTime::now()),
        }
    }

    pub(crate) fn measure(&self, measurement: T, attrs: AttributeSet) {
        self.value_map.measure(measurement, attrs)
    }

    pub(crate) fn delta(
        &self,
        dest: Option<&mut dyn Aggregation>,
    ) -> (usize, Option<Box<dyn Aggregation>>) {
        let t = SystemTime::now();

        let s_data = dest.and_then(|d| d.as_mut().downcast_mut::<data::Sum<T>>());
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                temporality: Temporality::Delta,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.temporality = Temporality::Delta;
        s_data.is_monotonic = self.monotonic;
        s_data.data_points.clear();

        let mut values = match self.value_map.values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let mut bound_values = match self.value_map.bound_values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let n = values.len() + bound_values.len();
        let mut processed_attr_sets = HashSet::with_capacity(values.len());
        if n > s_data.data_points.capacity() {
            s_data
                .data_points
                .reserve_exact(n - s_data.data_points.capacity());
        }

        let prev_start = self.start.lock().map(|start| *start).unwrap_or(t);
        for (attrs, value) in values.drain() {
            let mut data_point = DataPoint {
                attributes: attrs.clone(),
                start_time: Some(prev_start),
                time: Some(t),
                value,
                exemplars: vec![],
            };

            if let Some(bound_value) = bound_values.get(&attrs) {
                let value = bound_value.get_and_reset_value();
                data_point.value += value;
            }

            s_data.data_points.push(data_point);
            processed_attr_sets.insert(attrs);
        }

        // Add data points for any bound attribute sets that aren't in the unbound value map
        let mut bound_values_to_keep = HashMap::with_capacity(bound_values.len());
        for (attrs, value) in bound_values.drain() {
            if !processed_attr_sets.contains(&attrs) {
                s_data.data_points.push(DataPoint {
                    attributes: attrs.clone(),
                    start_time: Some(prev_start),
                    time: Some(t),
                    value: value.get_and_reset_value(),
                    exemplars: vec![],
                });
            }

            if Arc::strong_count(&value) > 1 {
                // There are still bound instruments using this tracker, so we need to keep it
                bound_values_to_keep.insert(attrs, value);
            }
        }

        // The delta collection cycle resets.
        if let Ok(mut start) = self.start.lock() {
            *start = t;
        }

        (n, new_agg.map(|a| Box::new(a) as Box<_>))
    }

    pub(crate) fn cumulative(
        &self,
        dest: Option<&mut dyn Aggregation>,
    ) -> (usize, Option<Box<dyn Aggregation>>) {
        let t = SystemTime::now();

        let s_data = dest.and_then(|d| d.as_mut().downcast_mut::<data::Sum<T>>());
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                temporality: Temporality::Cumulative,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.temporality = Temporality::Cumulative;
        s_data.is_monotonic = self.monotonic;
        s_data.data_points.clear();

        let values = match self.value_map.values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let mut bound_values = match self.value_map.bound_values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let n = values.len() + bound_values.len();
        if n > s_data.data_points.capacity() {
            s_data
                .data_points
                .reserve_exact(n - s_data.data_points.capacity());
        }

        let mut processed_attr_sets = HashSet::with_capacity(values.len());
        let prev_start = self.start.lock().map(|start| *start).unwrap_or(t);
        // TODO: This will use an unbounded amount of memory if there
        // are unbounded number of attribute sets being aggregated. Attribute
        // sets that become "stale" need to be forgotten so this will not
        // overload the system.
        for (attrs, value) in values.iter() {
            let mut data_point = DataPoint {
                attributes: attrs.clone(),
                start_time: Some(prev_start),
                time: Some(t),
                value: *value,
                exemplars: vec![],
            };

            if let Some(bound_value) = bound_values.get(attrs) {
                // TODO: For cumulative counts we need to add them to the original unbounded values
                // to persist them after the bounded values go away
                let value = bound_value.get_value();
                data_point.value += value;
            }

            s_data.data_points.push(data_point);
            processed_attr_sets.insert(attrs);
        }

        // Add data points for any bound attribute sets that aren't in the unbound value map
        let mut bound_values_to_keep = HashMap::with_capacity(bound_values.len());
        for (attrs, value) in bound_values.drain() {
            if !processed_attr_sets.contains(&attrs) {
                s_data.data_points.push(DataPoint {
                    attributes: attrs.clone(),
                    start_time: Some(prev_start),
                    time: Some(t),
                    value: value.get_value(),
                    exemplars: vec![],
                });
            }

            if Arc::strong_count(&value) > 1 {
                // There are still bound instruments using this tracker, so we need to keep it
                bound_values_to_keep.insert(attrs, value);
            }
        }

        for (attr, values) in bound_values_to_keep {
            bound_values.insert(attr, values);
        }

        (n, new_agg.map(|a| Box::new(a) as Box<_>))
    }
}

/// Summarizes a set of pre-computed sums as their arithmetic sum.
pub(crate) struct PrecomputedSum<T: Number<T>> {
    value_map: ValueMap<T>,
    monotonic: bool,
    start: Mutex<SystemTime>,
    reported: Mutex<HashMap<AttributeSet, T>>,
}

impl<T: Number<T>> PrecomputedSum<T> {
    pub(crate) fn new(monotonic: bool) -> Self {
        PrecomputedSum {
            value_map: ValueMap::new(),
            monotonic,
            start: Mutex::new(SystemTime::now()),
            reported: Mutex::new(Default::default()),
        }
    }

    pub(crate) fn measure(&self, measurement: T, attrs: AttributeSet) {
        self.value_map.measure(measurement, attrs)
    }

    pub(crate) fn delta(
        &self,
        dest: Option<&mut dyn Aggregation>,
    ) -> (usize, Option<Box<dyn Aggregation>>) {
        let t = SystemTime::now();
        let prev_start = self.start.lock().map(|start| *start).unwrap_or(t);

        let s_data = dest.and_then(|d| d.as_mut().downcast_mut::<data::Sum<T>>());
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                temporality: Temporality::Delta,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.data_points.clear();
        s_data.temporality = Temporality::Delta;
        s_data.is_monotonic = self.monotonic;

        let mut values = match self.value_map.values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let n = values.len();
        if n > s_data.data_points.capacity() {
            s_data
                .data_points
                .reserve_exact(n - s_data.data_points.capacity());
        }
        let mut new_reported = HashMap::with_capacity(n);
        let mut reported = match self.reported.lock() {
            Ok(r) => r,
            Err(_) => return (0, None),
        };

        let default = T::default();
        for (attrs, value) in values.drain() {
            let delta = value - *reported.get(&attrs).unwrap_or(&default);
            if delta != default {
                new_reported.insert(attrs.clone(), value);
            }
            s_data.data_points.push(DataPoint {
                attributes: attrs.clone(),
                start_time: Some(prev_start),
                time: Some(t),
                value: delta,
                exemplars: vec![],
            });
        }

        // The delta collection cycle resets.
        if let Ok(mut start) = self.start.lock() {
            *start = t;
        }

        *reported = new_reported;
        drop(reported); // drop before values guard is dropped

        (n, new_agg.map(|a| Box::new(a) as Box<_>))
    }

    pub(crate) fn cumulative(
        &self,
        dest: Option<&mut dyn Aggregation>,
    ) -> (usize, Option<Box<dyn Aggregation>>) {
        let t = SystemTime::now();
        let prev_start = self.start.lock().map(|start| *start).unwrap_or(t);

        let s_data = dest.and_then(|d| d.as_mut().downcast_mut::<data::Sum<T>>());
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                temporality: Temporality::Cumulative,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.data_points.clear();
        s_data.temporality = Temporality::Cumulative;
        s_data.is_monotonic = self.monotonic;

        let values = match self.value_map.values.lock() {
            Ok(v) => v,
            Err(_) => return (0, None),
        };

        let n = values.len();
        if n > s_data.data_points.capacity() {
            s_data
                .data_points
                .reserve_exact(n - s_data.data_points.capacity());
        }
        let mut new_reported = HashMap::with_capacity(n);
        let mut reported = match self.reported.lock() {
            Ok(r) => r,
            Err(_) => return (0, None),
        };

        let default = T::default();
        for (attrs, value) in values.iter() {
            let delta = *value - *reported.get(attrs).unwrap_or(&default);
            if delta != default {
                new_reported.insert(attrs.clone(), *value);
            }
            s_data.data_points.push(DataPoint {
                attributes: attrs.clone(),
                start_time: Some(prev_start),
                time: Some(t),
                value: delta,
                exemplars: vec![],
            });
        }

        *reported = new_reported;
        drop(reported); // drop before values guard is dropped

        (n, new_agg.map(|a| Box::new(a) as Box<_>))
    }
}

pub(crate) fn generate_bound_measure_sum<T: Number<T>>(
    sum: &Arc<Sum<T>>,
    attrs: AttributeSet,
) -> Arc<dyn BoundedMeasure<T>> {
    let tracker = sum.value_map.get_atomic_tracker(attrs);
    Arc::new(move |measurement: T| {
        tracker.add(measurement);
    })
}

pub(crate) fn generate_bound_measure_precomputed_sum<T: Number<T>>(
    precomputed_sum: &Arc<PrecomputedSum<T>>,
    attrs: AttributeSet,
) -> Arc<dyn BoundedMeasure<T>> {
    let tracker = precomputed_sum.value_map.get_atomic_tracker(attrs);
    Arc::new(move |measurement: T| {
        tracker.add(measurement);
    })
}
