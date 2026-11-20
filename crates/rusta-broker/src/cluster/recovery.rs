use super::*;

pub(super) struct RecoveryOffer {
    leader: u64,
    term: u64,
    committed_index: u64,
}

impl Engine {
    pub fn request_cache_recovery(&mut self, source: u64, key: GroupId, token: Uuid) -> Result<()> {
        if !self.settings.page_cache || key.topic.is_nil() || self.retired.contains(&source) {
            return Ok(());
        }
        let Some(node) = self.nodes.get_mut(&key) else {
            return Ok(());
        };
        let term = node.raw.raft.term;
        if node.cache_flushed_at.is_none() || !node.local_leader() || node.committed_term != term {
            return Ok(());
        }
        let Some(progress) = node.raw.raft.mut_prs().get_mut(source) else {
            return Ok(());
        };
        let Some(queue) = self.outbound.get(&source) else {
            return Ok(());
        };
        if node.recovery_requests.insert(source, (token, term)) != Some((token, term)) {
            progress.matched = 0;
            progress.become_probe();
            // become_probe can preserve the index of an in-flight snapshot.
            progress.next_idx = 1;
        } else if progress.state != raft::ProgressState::Snapshot {
            progress.become_probe();
        }
        progress.recent_active = true;
        Queued::send(
            queue,
            &self.send_bytes,
            Packet::RecoveryStart(key, token, term, node.log().committed().next_index - 1),
        );
        node.raw.raft.send_append(source);
        self.drain(key)
    }

    pub fn accept_cache_recovery(
        &mut self,
        source: u64,
        key: GroupId,
        token: Uuid,
        term: u64,
        committed_index: u64,
    ) {
        if self.settings.cache_recovery != Some(token)
            || self.retired.contains(&source)
            || !self.replicas(key).contains(&source)
        {
            return;
        }
        if let Some(node) = self.nodes.get_mut(&key)
            && node.recovering
            && term >= node.raw.raft.term
            && committed_index > 0
            && node
                .recovery_offer
                .as_ref()
                .is_none_or(|offer| term > offer.term)
        {
            node.recovery_offer = Some(RecoveryOffer {
                leader: source,
                term,
                committed_index,
            });
        }
    }

    pub(super) fn drive_cache_recovery(&mut self) -> Result<()> {
        let Some(token) = self.settings.cache_recovery else {
            return Ok(());
        };
        let mut waiting = false;
        for (&key, node) in &self.nodes {
            if !node.recovering {
                continue;
            }
            waiting = true;
            for peer in self.replicas(key) {
                if let Some(queue) = self.outbound.get(peer) {
                    Queued::send(queue, &self.send_bytes, Packet::RecoveryRequest(key, token));
                }
            }
        }
        if !waiting {
            // All recovering logs used durable writes until this point.
            let root = self.data.parent().expect("broker data root");
            super::super::finish_page_cache_recovery(root, super::super::kernel_boot_id()?)?;
            self.settings.cache_recovery = None;
            for node in self.nodes.values_mut() {
                if node.cache_flushed_at.is_some() {
                    node.raw.mut_store().log.enable_page_cache();
                }
            }
            eprintln!("page-cache replica recovery complete");
        }
        Ok(())
    }
}

impl Node {
    pub(super) fn accepts_recovery_append(&self, message: &Message) -> bool {
        // A heartbeat can advertise a match position from before the reboot.
        // Only an append or snapshot checks the surviving log before advancing it.
        matches!(
            message.get_msg_type(),
            MessageType::MsgAppend | MessageType::MsgSnapshot
        ) && self
            .recovery_offer
            .as_ref()
            .is_some_and(|offer| message.from == offer.leader && message.term == offer.term)
    }

    pub(super) fn check_recovery_progress(&mut self) {
        if self.recovery_offer.as_ref().is_some_and(|offer| {
            self.leader() == offer.leader
                && self.raw.raft.term == offer.term
                && self.committed_term == offer.term
                && self.log().committed().next_index > offer.committed_index
        }) {
            self.recovering = false;
            self.recovery_offer = None;
        }
    }
}
