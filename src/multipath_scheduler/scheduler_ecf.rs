use crate::connection::path::PathMap;
use crate::connection::space::PacketNumSpaceMap;
use crate::connection::stream::StreamMap;
use crate::multipath_scheduler::MultipathScheduler;
use crate::Error;
use crate::MultipathConfig;
use crate::Result;


/// The ECF (Earliest Completion First) scheduler.
///
/// This scheduler attempts to minimize the total completion time of data currently
/// pending for transmission. It achieves this by comparing the estimated
/// completion times of two decisions:
/// 1. Wait for the currently fastest (but possibly busy) path to become available.
/// 2. Immediately use a currently available (but possibly slower) path.
pub struct EcfScheduler {
    /// Hysteresis state flag, used to prevent the scheduling decision from
    /// oscillating between "wait" and "don't wait". If `true`, it indicates
    /// that the last decision was to "wait for the fast path".
    waiting_for_fast_path: bool,
}


impl EcfScheduler {
    pub fn new(_conf: &MultipathConfig) -> Self {
        EcfScheduler {
            waiting_for_fast_path: false,
        }
    }


    fn find_fastest_path(&self, paths: &PathMap) -> Option<usize> {
        paths
            .iter()
            // Only active and validated paths can participate in scheduling.
            .filter(|(_, p)| p.active() && p.validated())
            // Compares smoothed_rtt to find the minimum.
            .min_by_key(|(_, p)| p.recovery.rtt.smoothed_rtt())
            // Returns the path ID.
            .map(|(pid, _)| pid)
    }

    fn find_best_available_path(&self, paths: &mut PathMap) -> Option<usize> {
       let mut best_path: Option<(usize, std::time::Duration)> = None;

        // The for loop correctly binds p as &mut Path.
        for (pid, p) in paths.iter_mut() {
            if p.active() && p.validated() && p.recovery.can_send() {
                let rtt = p.recovery.rtt.smoothed_rtt();
            
                // Find the available path with the minimum RTT.
                if let Some((_, best_rtt)) = best_path {
                    if rtt < best_rtt {
                        best_path = Some((pid, rtt));
                    }
                } else {
                    best_path = Some((pid, rtt));
                }
            }
        }
        best_path.map(|(pid, _)| pid)
    }

    fn get_k_for_ecf(&self, paths: &PathMap) -> u64 {
        paths
            .iter()
            .filter(|(_, p)| p.active() && p.validated())
            .map(|(_, p)| p.recovery.congestion.congestion_window())
            .sum()
    }
}


impl MultipathScheduler for EcfScheduler {
    /// Selects a sending path based on the "Earliest Completion First" principle.
    fn on_select(
        &mut self,
        paths: &mut PathMap,
        _spaces: &mut PacketNumSpaceMap,
        // Note: According to the adopted scheme, the `streams` parameter may no
        // longer be needed, but it is kept to conform to the Trait definition.
        _streams: &mut StreamMap,
    ) -> Result<usize> {
        // Step 1: Find the theoretically fastest path (xf), regardless of its
        // current availability.
        let fastest_path_pid = match self.find_fastest_path(paths) {
            Some(pid) => pid,
            // If there are no active/validated paths, scheduling is not possible.
            None => return Err(Error::Done),
        };

        if paths.get_mut(fastest_path_pid)?.recovery.can_send() {
            self.waiting_for_fast_path = false;
            return Ok(fastest_path_pid);
        }

        // Step 3: The fastest path is busy. Find the best available, but
        // slower, path (xs).
        let slow_path_pid = match self.find_best_available_path(paths) {
            Some(pid) => pid,
            None => {
                // All paths are busy, there is no choice but to wait.
                self.waiting_for_fast_path = true;
                return Err(Error::Done);
            }
        };
        /*  // If the "available" path found is the fastest path itself (although
        // logically it should be can_send() == false, this is defensive
        // programming), it means there are no other available paths, so we must
        // wait.
        if fastest_path_pid == slow_path_pid {
            self.waiting_for_fast_path = true;
            return Err(Error::Done);
        } */
        
        // Step 4: Get the key parameters required for the ECF calculation.
        // **Key change**: `k` is no longer the sum of data to be sent across all
        // streams, but is based on the total congestion window of all paths.
        let k_bytes_to_send = self.get_k_for_ecf(paths);
        
        // If the connection's theoretical sending capacity is 0, scheduling is
        // not possible.
        if k_bytes_to_send == 0 {
             return Err(Error::Done);
        }

        // The following get() calls are safe because we have already confirmed
        // the existence of the pids.
        let fastest_path = paths.get(fastest_path_pid)?;
        let fastest_rtt = fastest_path.recovery.rtt.smoothed_rtt();
        let fastest_cwnd = fastest_path.recovery.congestion.congestion_window();

        let slow_path = paths.get(slow_path_pid)?;
        let slow_rtt = slow_path.recovery.rtt.smoothed_rtt();
        let slow_cwnd = slow_path.recovery.congestion.congestion_window();

        // Avoid division by zero. If any path has a CWND of 0, a meaningful
        // comparison cannot be made, so use the slow path directly.
        if fastest_cwnd == 0 || slow_cwnd == 0 {
            self.waiting_for_fast_path = false;
            return Ok(slow_path_pid);
        }

        // Step 5: Execute the core ECF decision logic.
        // To avoid floating-point arithmetic, we transform the formula from the
        // paper `RTTf + k/CWNDf * RTTf < RTTs` into integer multiplication:
        // `(CWNDf + k) * RTTf < CWNDf * RTTs`.
        // We use u128 to prevent overflow during calculation.
        let lhs = (fastest_cwnd as u128 + k_bytes_to_send as u128)
            .saturating_mul(fastest_rtt.as_micros());
        let mut rhs = (fastest_cwnd as u128).saturating_mul(slow_rtt.as_micros());

        // **Apply Hysteresis**:
        // If we are already in a "waiting" state, to prevent decision
        // oscillation, we add a "cost" to using the slow path. Here, we
        // increase `rhs` by 25%, making the inequality harder to satisfy, thus
        // favoring "continue to wait". This corresponds to the (1 + beta)
        // factor in the original C code implementation, where beta is 0.25.
        if self.waiting_for_fast_path {
            rhs = rhs.saturating_add(rhs / 4);
        }

        if lhs < rhs {
            // Initial calculation suggests "waiting" is better. Now, perform a
            // sanity check. The purpose of the sanity check is to only wait if
            // the cost of "using the slow path" is **significantly higher**
            // than "waiting".
            //
            // Paper's formula: `k/CWNDs * RTTs >= 2 * RTTf`
            // Integer form: `k * RTTs >= 2 * RTTf * CWNDs`
            let sanity_lhs = (k_bytes_to_send as u128).saturating_mul(slow_rtt.as_micros());
            let sanity_rhs = (2 as u128)
                .saturating_mul(fastest_rtt.as_micros())
                .saturating_mul(slow_cwnd as u128);

            if sanity_lhs >= sanity_rhs {
                // The check passed, confirming the cost of using the slow path
                // is very high.
                // **Final decision: Wait**
                self.waiting_for_fast_path = true;
                // Return Done to signal to the upper layer not to send a packet
                // at this time.
                return Err(Error::Done);
            }
        }

        // **Final decision: Use the slow path immediately**
        self.waiting_for_fast_path = false;
        Ok(slow_path_pid)
    }
}