use std::cmp;
use std::time::{Duration, Instant};

use crate::connection::path::{Path, PathMap};
use crate::connection::space::PacketNumSpaceMap;
use crate::connection::stream::StreamMap;
use crate::multipath_scheduler::{MultipathScheduler, MultipathAlgorithm}; // Assuming MinRttScheduler is accessible
use crate::multipath_scheduler::scheduler_minrtt::MinRttScheduler; // Import MinRtt
use crate::{Error, MultipathConfig, Result, CongestionControlAlgorithm};
use log::debug;
use crate::PathEvent; 

// Constants for lambda adjustments (can be made configurable later)
const BLEST_INITIAL_LAMBDA_SCALED: i16 = 1200; // lambda = 1.2 (scaled by 1000)
const BLEST_MIN_LAMBDA_SCALED: i16 = 1000;    // min_lambda = 1.0
const BLEST_MAX_LAMBDA_SCALED: i16 = 1500;    // max_lambda = 1.5 (adjust as needed)
const BLEST_DYN_LAMBDA_GOOD_SCALED: i16 = 50; // decrease by 0.05
const BLEST_DYN_LAMBDA_BAD_SCALED: i16 = 200; // increase by 0.2


// Time interval for lambda update, e.g., related to RTT
const BLEST_LAMBDA_UPDATE_INTERVAL_DIVISOR: u32 = 4; // Update lambda roughly every RTT/4 of the slow path



pub struct BlestScheduler {
    // Underlying scheduler to get an initial path choice (e.g., MinRTT)
    default_scheduler: Box<dyn MultipathScheduler>, // Or directly MinRttScheduler if preferred

    // Per-connection state for BLEST
    lambda_scaled: i16, // Current lambda value, scaled by 1000
    last_lambda_update_time: Instant,
    // To track losses on the chosen slow path for lambda adjustment
    last_chosen_slow_path_id: Option<usize>,
    last_chosen_slow_path_lost_count: u64,
    last_slow_path_rtt_micros: u64, // For lambda update interval
}



impl BlestScheduler {
     pub fn new(conf: &MultipathConfig) -> Self {
        // You might want to allow choosing the default scheduler via config too.
        // For now, let's assume MinRtt as the default if BLEST is chosen.
        let default_scheduler = Box::new(MinRttScheduler::new(conf));

        BlestScheduler {
            default_scheduler,
            lambda_scaled: BLEST_INITIAL_LAMBDA_SCALED,
            last_lambda_update_time: Instant::now(),
            last_chosen_slow_path_id: None,
            last_chosen_slow_path_lost_count: 0,
            last_slow_path_rtt_micros: 0,
        }
    }

    /// Updates lambda based on performance of the previously chosen slow path.
    fn update_lambda(&mut self, paths: &PathMap, now: Instant) {
        if let Some(path_id) = self.last_chosen_slow_path_id {
            if let Ok(slow_path) = paths.get(path_id) {
                 // Check if enough time has passed since the last update
                let min_update_interval = if self.last_slow_path_rtt_micros > 0 {
                    Duration::from_micros(self.last_slow_path_rtt_micros / BLEST_LAMBDA_UPDATE_INTERVAL_DIVISOR as u64)
                } else {
                    Duration::from_millis(50) // Default if RTT was 0
                };

                if now.saturating_duration_since(self.last_lambda_update_time) < min_update_interval {
                    return;
                }

                let current_lost_count = slow_path.recovery.stats.lost_count;
                if current_lost_count > self.last_chosen_slow_path_lost_count {
                    // Losses occurred on the slow path
                    self.lambda_scaled += BLEST_DYN_LAMBDA_BAD_SCALED;
                    debug!("BLEST: Lambda increased due to losses on path {}", path_id);
                } else {
                    // No new losses on the slow path
                    self.lambda_scaled -= BLEST_DYN_LAMBDA_GOOD_SCALED;
                    debug!("BLEST: Lambda decreased for path {}", path_id);
                }

                self.lambda_scaled = cmp::max(BLEST_MIN_LAMBDA_SCALED, cmp::min(self.lambda_scaled, BLEST_MAX_LAMBDA_SCALED));
                self.last_lambda_update_time = now;
                // Reset for next observation period or update
                self.last_chosen_slow_path_lost_count = current_lost_count;
                self.last_slow_path_rtt_micros = slow_path.recovery.rtt.smoothed_rtt().as_micros() as u64;
            }
        }
         // If no slow path was previously chosen, or path disappeared, lambda doesn't change this cycle based on it.
        // We could also have a general time-based decay if no slow path is chosen for a while.
    }


    /// Estimates how many bytes the `fast_path` could send during the `duration`.
    /// This is a simplified estimation for QUIC.
    fn estimate_fast_path_bytes(&self, fast_path: &Path, duration: Duration) -> u64 {
        if duration.is_zero() {
            return 0;
        }

        let fast_path_srtt = fast_path.recovery.rtt.smoothed_rtt();
        if fast_path_srtt.is_zero() {
            // If SRTT is zero, path is likely new or unused, estimate aggressively based on CWND.
            // This is a rough guess.
            return fast_path.recovery.congestion.congestion_window();
        }

        // Bytes per RTT for the fast path (BDP-like)
        let bytes_per_rtt_fast = fast_path.recovery.congestion.congestion_window();

        // How many "fast path RTTs" fit into the given duration
        let num_fast_rtts = duration.as_micros() as f64 / fast_path_srtt.as_micros() as f64;

        let estimated_bytes = (bytes_per_rtt_fast as f64 * num_fast_rtts).round() as u64;

        // Apply lambda scaling
        (estimated_bytes * self.lambda_scaled as u64) / 1000
    }

}
impl MultipathScheduler for BlestScheduler {
    fn on_select(
        &mut self,
        paths: &mut PathMap,
        spaces: &mut PacketNumSpaceMap,
        streams: &mut StreamMap,
    ) -> Result<usize> {

        let now = Instant::now();
        let default_path_id = match self.default_scheduler.on_select(paths, spaces, streams) {
            Ok(id) => id,
            Err(e) => return Err(e), // No path available at all
        };
        let default_path_srtt = paths.get(default_path_id)?.recovery.rtt.smoothed_rtt();
        //let default_path = paths.get(default_path_id)?;
        // 2. Find the "fastest" currently available path
        let mut fastest_path_id_opt: Option<usize> = None;
        let mut min_srtt = Duration::MAX;
        for (id, path) in paths.iter_mut() {
            if id == default_path_id || !path.active() || !path.recovery.can_send() {
                continue;
            }
            // Ensure path has a valid DCID to be usable
            if path.dcid_seq.is_none() {
                continue;
            }

            let srtt = path.recovery.rtt.smoothed_rtt();
            if srtt < min_srtt && !srtt.is_zero() {
                min_srtt = srtt;
                fastest_path_id_opt = Some(id);
            }
        }
            
         // 3. If default path is already the fastest, or no other faster path exists, use it.
        let fastest_path_id = match fastest_path_id_opt {
            Some(id) if default_path_srtt > min_srtt => id,
            _ => {
                // Default path is fastest or no other faster path, or default path RTT is not worse.
                // Reset last_chosen_slow_path_id as we are not choosing a "slow" path guarded by BLEST
                self.last_chosen_slow_path_id = None;
                return Ok(default_path_id);
            }
        };

        // 4. BLEST Logic: Default path is slower than `fastest_path_id`
        let slow_path = paths.get(default_path_id)?;
        let fast_path = paths.get(fastest_path_id)?;

        // Update lambda based on previous observation of the *current candidate* slow_path
        // This is a simplification: MPTCP BLEST looked at retransmissions specific to the slow flow.
        // Here, we check if the *candidate slow_path for this decision* had recent losses.
        if self.last_chosen_slow_path_id == Some(default_path_id) {
             self.update_lambda(paths, now); // Update lambda based on previous cycle's slow path
        } else {
            // This is the first time we are considering this path as "slow"
            // or a different slow path was chosen last time.
            // We need to re-initialize observation for *this* slow_path for the *next* lambda update.
            self.last_chosen_slow_path_id = Some(default_path_id);
            self.last_chosen_slow_path_lost_count = slow_path.recovery.stats.lost_count;
            self.last_slow_path_rtt_micros = slow_path.recovery.rtt.smoothed_rtt().as_micros() as u64;
            self.last_lambda_update_time = now; // Reset update time for this new observation
        }
        
         // Linger time for data on the slow path (simplified to its SRTT)
        let slow_path_linger_duration = slow_path.recovery.rtt.smoothed_rtt();
        if slow_path_linger_duration.is_zero() {
            // Avoid division by zero or nonsensical linger time; use default path if RTT is unknown
            return Ok(default_path_id);
        }
        let fast_path_potential_bytes = self.estimate_fast_path_bytes(fast_path, slow_path_linger_duration);
        // Available connection-level send window for the fast path
        // if we also commit to sending one packet on the slow path.
        let conn_send_capacity_max_data = streams.conn_max_tx_data(); 
        let conn_send_capacity_tx_data = streams.conn_tx_data();  
        let conn_flow_control_available = conn_send_capacity_max_data.saturating_sub(conn_send_capacity_tx_data);
        
         // The critical resource is the connection's flow control window.
        // If we send on the slow path, it uses up some of this window.
        // The remaining window must be sufficient for what the fast path *could* send.
        let packet_size_on_slow_path = slow_path.recovery.max_datagram_size as u64;

        // Available window for the fast path if we also send one packet on the slow path.
        let conn_window_if_slow_sends = conn_flow_control_available.saturating_sub(packet_size_on_slow_path);


        debug!(
            "BLEST: SlowPath ({}) SRTT: {:?}, FastPath ({}) SRTT: {:?}",
            default_path_id, slow_path.recovery.rtt.smoothed_rtt(),
            fastest_path_id, fast_path.recovery.rtt.smoothed_rtt()
        );
        debug!(
            "BLEST: FastPath potential: {} bytes, ConnAvailIfSlowSends: {} bytes (TotalConnAvail: {}, Lambda: {})",
            fast_path_potential_bytes, conn_window_if_slow_sends, conn_flow_control_available, self.lambda_scaled
        );

        
         if fast_path_potential_bytes > conn_window_if_slow_sends {
            // Sending on the slow path would starve the fast path due to connection-level flow control.
            // So, don't send on the slow path *now*.
            // The connection should still try to send *something*, so it might pick the fast path,
            // or if only the slow path was viable from default scheduler, it means we wait.
            debug!(
                "BLEST: VETOING slow path {}. Fast potential {} > avail_if_slow_sends {}",
                default_path_id, fast_path_potential_bytes, conn_window_if_slow_sends
            );

            // Critical: If we veto the slow path, should we try the fast path instead?
            // Or just signal "don't send on *this* slow path now"?
            // The original MPTCP BLEST returned NULL, meaning the scheduler overall decided not to send.
            // For TQUIC, returning `Err(Error::Done)` tells the connection send loop that no path was selected by the scheduler.
            // The send loop might then try to find *any* sendable packet (e.g. ACK, PING) or just wait.
            // This seems like the correct equivalent of MPTCP's NULL.
            return Err(Error::Done);
        }
        

        // BLEST allows sending on the (slower) default path.
        // Record this choice for the next lambda update.
        // (Already done above when `last_chosen_slow_path_id` was set/checked)
        debug!("BLEST: ALLOWING slow path {}", default_path_id);
        Ok(default_path_id)
    }


     fn on_sent(
        &mut self,
        _packet: &crate::connection::space::SentPacket,
        _now: Instant,
        _path_id: usize,
        _paths: &mut PathMap,
        _spaces: &mut PacketNumSpaceMap,
        _streams: &mut StreamMap,
    ) {
        // The lambda update is tied to observations *before* making a selection.
        // However, if `on_sent` is called for the `last_chosen_slow_path_id`,
        // we could potentially refine the `last_chosen_slow_path_lost_count` here
        // if the packet itself contains retransmitted data, but it's simpler to
        // rely on the cumulative `lost_count` at the next `on_select`.
    }
   
    fn on_path_updated(&mut self, paths: &mut PathMap, event: PathEvent) {
       
        self.default_scheduler.on_path_updated(paths, event);

       
        if let Some(last_slow_id) = self.last_chosen_slow_path_id {
            match event {
               
                PathEvent::Abandoned(abandoned_id) => { 
                    if abandoned_id == last_slow_id {
                       
                        self.last_chosen_slow_path_id = None;
                        self.last_chosen_slow_path_lost_count = 0;
                        self.last_slow_path_rtt_micros = 0;
                        debug!("BLEST: Last chosen slow path {} was abandoned, resetting observation.", abandoned_id);
                    }
                }
              
                PathEvent::Validated(validated_id) => {
                    
                }
            }
        }
    }
}




