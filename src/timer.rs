/// A scope based timer.
use std::time::Instant;
use log::debug;

pub(crate) struct Timer {
    start: Instant,
    name: String,
}

impl Timer {
    pub(crate) fn start(name: &str) -> Self {
        Self {
            name: name.into(),
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        debug!(
            "[timer] {} : {}ms",
            self.name,
            self.start.elapsed().as_millis()
        );
    }
}
