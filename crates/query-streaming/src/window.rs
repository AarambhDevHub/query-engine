//! Window implementations for streaming queries
//!
//! Provides different window types for grouping stream data.

use std::time::{Duration, Instant};

/// Trait for window implementations
pub trait Window: Send + Sync {
    /// Check if the window should trigger (emit results)
    fn should_trigger(&self) -> bool;

    /// Advance the window after triggering
    fn advance(&mut self);

    /// Get the window start time
    fn start(&self) -> Instant;

    /// Get the window end time (if bounded)
    fn end(&self) -> Option<Instant>;

    /// Reset the window
    fn reset(&mut self);
}

/// Types of windows supported
#[derive(Debug, Clone)]
pub enum WindowType {
    /// Fixed-size non-overlapping windows
    Tumbling { size: Duration },
    /// Overlapping windows with size and slide
    Sliding { size: Duration, slide: Duration },
    /// Gap-based session windows
    Session { gap: Duration },
}

impl WindowType {
    /// Create a tumbling window
    pub fn tumbling(size: Duration) -> Self {
        Self::Tumbling { size }
    }

    /// Create a sliding window
    pub fn sliding(size: Duration, slide: Duration) -> Self {
        Self::Sliding { size, slide }
    }

    /// Create a session window
    pub fn session(gap: Duration) -> Self {
        Self::Session { gap }
    }

    /// Create a window instance from this type
    pub fn create_window(&self) -> Box<dyn Window> {
        match self {
            WindowType::Tumbling { size } => Box::new(TumblingWindow::new(*size)),
            WindowType::Sliding { size, slide } => Box::new(SlidingWindow::new(*size, *slide)),
            WindowType::Session { gap } => Box::new(SessionWindow::new(*gap)),
        }
    }
}

/// Tumbling window - fixed-size, non-overlapping windows
#[derive(Debug)]
pub struct TumblingWindow {
    size: Duration,
    start_time: Instant,
}

impl TumblingWindow {
    /// Create a new tumbling window
    pub fn new(size: Duration) -> Self {
        Self {
            size,
            start_time: Instant::now(),
        }
    }
}

impl Window for TumblingWindow {
    fn should_trigger(&self) -> bool {
        self.start_time.elapsed() >= self.size
    }

    fn advance(&mut self) {
        self.start_time = Instant::now();
    }

    fn start(&self) -> Instant {
        self.start_time
    }

    fn end(&self) -> Option<Instant> {
        Some(self.start_time + self.size)
    }

    fn reset(&mut self) {
        self.start_time = Instant::now();
    }
}

/// Sliding window - overlapping windows with size and slide interval
#[derive(Debug)]
pub struct SlidingWindow {
    size: Duration,
    slide: Duration,
    start_time: Instant,
    last_slide: Instant,
}

impl SlidingWindow {
    /// Create a new sliding window
    pub fn new(size: Duration, slide: Duration) -> Self {
        let now = Instant::now();
        Self {
            size,
            slide,
            start_time: now,
            last_slide: now,
        }
    }
}

impl Window for SlidingWindow {
    fn should_trigger(&self) -> bool {
        self.last_slide.elapsed() >= self.slide
    }

    fn advance(&mut self) {
        self.last_slide = Instant::now();
        // Window start slides forward by slide amount
        if self.start_time.elapsed() >= self.size {
            self.start_time = Instant::now() - (self.size - self.slide);
        }
    }

    fn start(&self) -> Instant {
        self.start_time
    }

    fn end(&self) -> Option<Instant> {
        Some(self.start_time + self.size)
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.start_time = now;
        self.last_slide = now;
    }
}

/// Session window - gap-based grouping
#[derive(Debug)]
pub struct SessionWindow {
    gap: Duration,
    start_time: Instant,
    last_event: Instant,
}

impl SessionWindow {
    /// Create a new session window
    pub fn new(gap: Duration) -> Self {
        let now = Instant::now();
        Self {
            gap,
            start_time: now,
            last_event: now,
        }
    }

    /// Record an event (resets gap timer)
    pub fn record_event(&mut self) {
        self.last_event = Instant::now();
    }
}

impl Window for SessionWindow {
    fn should_trigger(&self) -> bool {
        // Session ends when gap is exceeded
        self.last_event.elapsed() >= self.gap
    }

    fn advance(&mut self) {
        // Start new session
        let now = Instant::now();
        self.start_time = now;
        self.last_event = now;
    }

    fn start(&self) -> Instant {
        self.start_time
    }

    fn end(&self) -> Option<Instant> {
        // Session windows don't have a fixed end
        None
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.start_time = now;
        self.last_event = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_tumbling_window() {
        let mut window = TumblingWindow::new(Duration::from_millis(50));

        assert!(!window.should_trigger());

        thread::sleep(Duration::from_millis(60));

        assert!(window.should_trigger());

        window.advance();
        assert!(!window.should_trigger());
    }

    #[test]
    fn test_window_type_create() {
        let tumbling = WindowType::tumbling(Duration::from_secs(10));
        let sliding = WindowType::sliding(Duration::from_secs(10), Duration::from_secs(5));
        let session = WindowType::session(Duration::from_secs(30));

        let _w1 = tumbling.create_window();
        let _w2 = sliding.create_window();
        let _w3 = session.create_window();
    }
}
