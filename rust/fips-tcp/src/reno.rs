#[derive(Clone, Debug)]
pub(crate) struct Reno {
    cwnd: usize,
    mss: usize,
    ssthresh: usize,
    in_fast_recovery: bool,
    in_rto_recovery: bool,
}

impl Reno {
    pub(crate) fn new(mss: usize) -> Self {
        Self {
            cwnd: mss.saturating_mul(2),
            mss,
            ssthresh: usize::MAX,
            in_fast_recovery: false,
            in_rto_recovery: false,
        }
    }

    pub(crate) fn window(&self) -> usize {
        self.cwnd.max(self.mss)
    }

    pub(crate) fn set_mss(&mut self, mss: usize) {
        self.mss = mss.max(1);
        self.cwnd = self.cwnd.max(self.mss);
    }

    pub(crate) fn on_ack(&mut self, acked: usize) {
        if acked == 0 {
            return;
        }
        self.in_rto_recovery = false;
        if self.in_fast_recovery {
            self.in_fast_recovery = false;
            self.cwnd = self.ssthresh;
            return;
        }
        let increment = if self.cwnd < self.ssthresh {
            acked.min(self.mss)
        } else {
            self.mss
                .saturating_mul(self.mss)
                .checked_div(self.cwnd)
                .unwrap_or(0)
                .max(1)
        };
        self.cwnd = self.cwnd.saturating_add(increment).max(self.mss);
    }

    pub(crate) fn on_duplicate_ack(&mut self) {
        if self.in_fast_recovery {
            self.cwnd = self.cwnd.saturating_add(self.mss);
        }
    }

    pub(crate) fn on_fast_loss(&mut self, in_flight: usize) {
        if self.in_fast_recovery {
            return;
        }
        self.ssthresh = (in_flight / 2).max(self.mss.saturating_mul(2));
        self.cwnd = self.ssthresh.saturating_add(self.mss.saturating_mul(3));
        self.in_fast_recovery = true;
    }

    pub(crate) fn on_timeout(&mut self, in_flight: usize) {
        if !self.in_rto_recovery {
            self.ssthresh = (in_flight / 2).max(self.mss.saturating_mul(2));
            self.in_rto_recovery = true;
        }
        self.cwnd = self.mss;
        self.in_fast_recovery = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_start_loss_and_recovery_match_reno_shape() {
        let mut reno = Reno::new(1000);
        assert_eq!(reno.window(), 2000);
        reno.on_ack(1000);
        assert_eq!(reno.window(), 3000);
        reno.on_timeout(3000);
        assert_eq!(reno.window(), 1000);
        reno.on_ack(1000);
        assert_eq!(reno.window(), 2000);
    }
}
