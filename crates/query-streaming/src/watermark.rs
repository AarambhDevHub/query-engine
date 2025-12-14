//! Watermark support for event-time processing
//!
//! Watermarks track the progress of event time and help
//! determine when windows can be closed.

use std::sync::atomic::{AtomicI64, Ordering};

/// Policy for handling late events
#[derive(Debug, Clone, PartialEq)]
pub enum LateEventPolicy {
    /// Drop late events silently
    Drop,
    /// Emit late events to a side output
    SideOutput,
    /// Allow late events up to a threshold
    Allow { max_lateness_ms: i64 },
}

impl Default for LateEventPolicy {
    fn default() -> Self {
        Self::Drop
    }
}

/// Watermark for tracking event-time progress
#[derive(Debug)]
pub struct Watermark {
    /// Current watermark timestamp (milliseconds since epoch)
    current: AtomicI64,
    /// Policy for late events
    late_policy: LateEventPolicy,
}

impl Watermark {
    /// Create a new watermark starting at 0
    pub fn new() -> Self {
        Self {
            current: AtomicI64::new(0),
            late_policy: LateEventPolicy::default(),
        }
    }

    /// Create a watermark with a starting timestamp
    pub fn with_timestamp(timestamp_ms: i64) -> Self {
        Self {
            current: AtomicI64::new(timestamp_ms),
            late_policy: LateEventPolicy::default(),
        }
    }

    /// Create a watermark with a late event policy
    pub fn with_policy(policy: LateEventPolicy) -> Self {
        Self {
            current: AtomicI64::new(0),
            late_policy: policy,
        }
    }

    /// Get the current watermark timestamp
    pub fn current(&self) -> i64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Advance the watermark to a new timestamp
    /// Only advances if the new timestamp is greater than current
    pub fn advance(&self, timestamp_ms: i64) -> bool {
        let current = self.current.load(Ordering::Relaxed);
        if timestamp_ms > current {
            self.current.store(timestamp_ms, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Check if an event is late based on its timestamp
    pub fn is_late(&self, event_timestamp_ms: i64) -> bool {
        event_timestamp_ms < self.current.load(Ordering::Relaxed)
    }

    /// Check if a late event should be allowed based on policy
    pub fn should_allow_late(&self, event_timestamp_ms: i64) -> bool {
        let current = self.current.load(Ordering::Relaxed);

        match &self.late_policy {
            LateEventPolicy::Drop => false,
            LateEventPolicy::SideOutput => false, // Handle separately
            LateEventPolicy::Allow { max_lateness_ms } => {
                current - event_timestamp_ms <= *max_lateness_ms
            }
        }
    }

    /// Get the late event policy
    pub fn policy(&self) -> &LateEventPolicy {
        &self.late_policy
    }

    /// Set the late event policy
    pub fn set_policy(&mut self, policy: LateEventPolicy) {
        self.late_policy = policy;
    }

    /// Reset the watermark to 0
    pub fn reset(&self) {
        self.current.store(0, Ordering::Relaxed);
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Watermark {
    fn clone(&self) -> Self {
        Self {
            current: AtomicI64::new(self.current.load(Ordering::Relaxed)),
            late_policy: self.late_policy.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_advance() {
        let watermark = Watermark::new();

        assert_eq!(watermark.current(), 0);

        assert!(watermark.advance(1000));
        assert_eq!(watermark.current(), 1000);

        // Can't go backwards
        assert!(!watermark.advance(500));
        assert_eq!(watermark.current(), 1000);

        // Can advance further
        assert!(watermark.advance(2000));
        assert_eq!(watermark.current(), 2000);
    }

    #[test]
    fn test_late_event_detection() {
        let watermark = Watermark::with_timestamp(1000);

        assert!(watermark.is_late(900));
        assert!(!watermark.is_late(1000));
        assert!(!watermark.is_late(1100));
    }

    #[test]
    fn test_late_event_policy_drop() {
        let watermark = Watermark::with_timestamp(1000);

        assert!(!watermark.should_allow_late(900));
    }

    #[test]
    fn test_late_event_policy_allow() {
        let mut watermark = Watermark::with_timestamp(1000);
        watermark.set_policy(LateEventPolicy::Allow {
            max_lateness_ms: 200,
        });

        // Within lateness threshold
        assert!(watermark.should_allow_late(850));

        // Beyond threshold
        assert!(!watermark.should_allow_late(700));
    }

    #[test]
    fn test_watermark_clone() {
        let original = Watermark::with_timestamp(5000);
        let cloned = original.clone();

        assert_eq!(cloned.current(), 5000);

        // Modifications don't affect each other
        original.advance(6000);
        assert_eq!(original.current(), 6000);
        assert_eq!(cloned.current(), 5000);
    }
}
