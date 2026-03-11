use anyhow::Result;
use std::io;
use std::time::Instant;

#[derive(Debug)]
pub struct TimingSnapshot {
    wall_start: Instant,
    usage_start: libc::rusage,
}

#[derive(Debug)]
pub struct TimerState {
    enabled: bool,
}

#[derive(Debug, PartialEq)]
pub struct TimingSummary {
    pub real_secs: f64,
    pub user_secs: f64,
    pub sys_secs: f64,
}

impl TimingSnapshot {
    pub fn start() -> io::Result<Self> {
        Ok(Self {
            wall_start: Instant::now(),
            usage_start: process_usage()?,
        })
    }

    pub fn finish(self) -> io::Result<TimingSummary> {
        let usage_end = process_usage()?;

        Ok(TimingSummary {
            real_secs: self.wall_start.elapsed().as_secs_f64(),
            user_secs: time_diff_secs(usage_end.ru_utime, self.usage_start.ru_utime),
            sys_secs: time_diff_secs(usage_end.ru_stime, self.usage_start.ru_stime),
        })
    }
}

impl TimerState {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn run<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        let timer = self.enabled.then(TimingSnapshot::start).transpose()?;
        let value = f()?;
        if let Some(timer) = timer {
            eprintln!("{}", timer.finish()?.format_sqlite());
        }
        Ok(value)
    }
}

impl TimingSummary {
    pub fn format_sqlite(&self) -> String {
        format!(
            "Run Time: real {:.3} user {:.6} sys {:.6}",
            self.real_secs, self.user_secs, self.sys_secs
        )
    }
}

fn process_usage() -> io::Result<libc::rusage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result == 0 {
        Ok(unsafe { usage.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

fn time_diff_secs(end: libc::timeval, start: libc::timeval) -> f64 {
    let end_secs = end.tv_sec as f64 + (end.tv_usec as f64 / 1_000_000.0);
    let start_secs = start.tv_sec as f64 + (start.tv_usec as f64 / 1_000_000.0);
    end_secs - start_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_timer_runs_without_timing() {
        let timer = TimerState::new(false);
        let value = timer.run(|| Ok::<_, anyhow::Error>(7)).unwrap();

        assert_eq!(value, 7);
    }

    #[test]
    fn formats_sqlite_timer_line() {
        let summary = TimingSummary {
            real_secs: 0.1234,
            user_secs: 0.4567,
            sys_secs: 0.0891,
        };

        assert_eq!(
            summary.format_sqlite(),
            "Run Time: real 0.123 user 0.456700 sys 0.089100"
        );
    }
}
