#[derive(Clone, Debug)]
pub(crate) struct RttEstimator {
    have_measurement: bool,
    srtt_ms: u64,
    rttvar_ms: u64,
    rto_ms: u64,
    min_rto_ms: u64,
    max_rto_ms: u64,
    consecutive_rtos: u8,
}

impl RttEstimator {
    pub(crate) fn new(initial_rto_ms: u64, min_rto_ms: u64, max_rto_ms: u64) -> Self {
        Self {
            have_measurement: false,
            srtt_ms: 0,
            rttvar_ms: 0,
            rto_ms: initial_rto_ms.clamp(min_rto_ms, max_rto_ms),
            min_rto_ms,
            max_rto_ms,
            consecutive_rtos: 0,
        }
    }

    pub(crate) fn timeout_ms(&self) -> u64 {
        self.rto_ms
    }

    pub(crate) fn sample(&mut self, sample_ms: u64) {
        let sample_ms = sample_ms.max(1);
        if self.have_measurement {
            let diff = self.srtt_ms.abs_diff(sample_ms);
            self.rttvar_ms = (self.rttvar_ms * 3 + diff).div_ceil(4);
            self.srtt_ms = (self.srtt_ms * 7 + sample_ms).div_ceil(8);
        } else {
            self.have_measurement = true;
            self.srtt_ms = sample_ms;
            self.rttvar_ms = sample_ms / 2;
        }
        let margin = 5.max(self.rttvar_ms.saturating_mul(4));
        self.rto_ms = self
            .srtt_ms
            .saturating_add(margin)
            .clamp(self.min_rto_ms, self.max_rto_ms);
        self.consecutive_rtos = 0;
    }

    pub(crate) fn on_timeout(&mut self) {
        self.rto_ms = self.rto_ms.saturating_mul(2).min(self.max_rto_ms);
        self.consecutive_rtos = self.consecutive_rtos.saturating_add(1);
        if self.consecutive_rtos >= 3 {
            self.have_measurement = false;
            self.consecutive_rtos = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimator_samples_and_backs_off() {
        let mut estimator = RttEstimator::new(1_000, 200, 60_000);
        estimator.sample(100);
        assert_eq!(estimator.timeout_ms(), 300);
        estimator.sample(120);
        assert!((200..=400).contains(&estimator.timeout_ms()));
        let before = estimator.timeout_ms();
        estimator.on_timeout();
        assert_eq!(estimator.timeout_ms(), before * 2);
    }
}
