//! Fortschrittsanzeige: pro Tabelle ein Balken (Phasen: Chunks vergleichen,
//! Drill-Down, Zeilen senden/löschen). Über Counts abschätzbar:
//!   - Buckets gesamt = Anzahl empfangener/lokaler Chunk-Hashes
//!   - Zeilen gesamt  = Differenzen nach Drill-Down

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

#[derive(Clone)]
pub struct Progress {
    pub multi: Option<Arc<MultiProgress>>,
    pub enabled: bool,
}

impl Progress {
    pub fn new(enabled: bool) -> Self {
        if enabled {
            Progress { multi: Some(Arc::new(MultiProgress::new())), enabled: true }
        } else {
            Progress { multi: None, enabled: false }
        }
    }

    pub fn bar(&self, label: &str, total: u64) -> TableBar {
        if !self.enabled {
            return TableBar { pb: None, label: label.to_string(), quiet_log: true };
        }
        let multi = self.multi.as_ref().unwrap();
        let pb = multi.add(ProgressBar::new(total));
        pb.set_style(
            ProgressStyle::with_template("  {prefix} {msg} {bar:28} {pos}/{len} ({eta})")
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=>-"),
        );
        pb.set_prefix(format!("[{label}]"));
        pb.set_message("start");
        TableBar { pb: Some(pb), label: label.to_string(), quiet_log: false }
    }
}

pub struct TableBar {
    pb: Option<ProgressBar>,
    pub label: String,
    quiet_log: bool,
}

impl TableBar {
    pub fn phase(&self, msg: &str) {
        match &self.pb {
            Some(pb) => {
                pb.set_length(0);
                pb.set_position(0);
                pb.set_message(msg.to_string());
            }
            None => {
                if self.quiet_log {
                    log::info!("[{}] {msg}", self.label);
                }
            }
        }
    }

    pub fn set_total(&self, total: u64, msg: &str) {
        match &self.pb {
            Some(pb) => {
                pb.set_length(total);
                pb.set_message(msg.to_string());
            }
            None => {
                if self.quiet_log {
                    log::info!("[{}] {msg} (0/{total})", self.label);
                }
            }
        }
    }

    pub fn inc(&self, n: u64) {
        if let Some(pb) = &self.pb {
            pb.inc(n);
        }
    }

    pub fn done(&self, msg: &str) {
        match &self.pb {
            Some(pb) => pb.finish_with_message(msg.to_string()),
            None => {
                if self.quiet_log {
                    log::info!("[{}] {msg}", self.label);
                }
            }
        }
    }

    pub fn fail(&self, msg: &str) {
        match &self.pb {
            Some(pb) => pb.abandon_with_message(format!("FEHLER: {msg}")),
            None => {
                if self.quiet_log {
                    log::error!("[{}] {msg}", self.label);
                }
            }
        }
    }
}
