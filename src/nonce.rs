use log::{debug, info, warn};
use serde_json::{json, Value};
use solana_client::{rpc_client::RpcClient, rpc_request::RpcRequest};
use solana_rpc_client_api::client_error::Error as RpcClientError;
use solana_sdk::{
    hash::Hash,
    nonce::state::State as NonceState,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::config::Config;

pub struct NonceManager {
    rpc: RpcClient,
    operator: Arc<Keypair>,
    config: Arc<Config>,
    pub nonce_accounts: Vec<PubkeySlot>,
    /// Cached blockhashes per slot index - the core cache that avoids RPC calls
    blockhash_cache: RwLock<HashMap<usize, Hash>>,
    /// Slots currently in-flight (transaction sent but not confirmed)
    in_flight: RwLock<HashSet<usize>>,
}

#[derive(Clone, Copy, Debug)]
pub struct PubkeySlot {
    pub index: usize,
    pub pubkey: Pubkey,
}

#[derive(Clone, Copy, Debug)]
pub struct PreparedNonce {
    pub account: Pubkey,
    pub blockhash: Hash,
}

#[derive(Clone, Copy, Debug)]
pub struct TxExecutionPlan {
    pub nonce: PreparedNonce,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_lamports: u64,
}

impl NonceManager {
    pub fn new(config: Arc<Config>) -> Result<Self, NonceError> {
        let rpc = RpcClient::new_with_timeout(config.rpc_url.clone(), Duration::from_secs(10));

        let mut nonce_accounts = Vec::new();
        for (i, maybe_pubkey) in config.nonce_accounts.iter().enumerate() {
            if let Some(pubkey) = maybe_pubkey {
                nonce_accounts.push(PubkeySlot {
                    index: i,
                    pubkey: *pubkey,
                });
            }
        }

        let mut manager = Self {
            rpc,
            operator: config.operator_keypair(),
            config,
            nonce_accounts,
            blockhash_cache: RwLock::new(HashMap::new()),
            in_flight: RwLock::new(HashSet::new()),
        };

        manager.ensure_nonce_accounts()?;
        manager.prime_blockhash_cache()?;

        Ok(manager)
    }

    /// Pre-fetches all nonce blockhashes into cache at startup.
    /// This is the only time we require RPC to succeed for nonces.
    fn prime_blockhash_cache(&self) -> Result<(), NonceError> {
        info!(
            "Priming nonce blockhash cache for {} slots",
            self.nonce_accounts.len()
        );
        let mut cache = self.blockhash_cache.write().unwrap();

        for entry in &self.nonce_accounts {
            let blockhash = self.fetch_nonce_blockhash_rpc(&entry.pubkey)?;
            info!(
                "Cached nonce slot {} | account {} | blockhash {}",
                entry.index, entry.pubkey, blockhash
            );
            cache.insert(entry.index, blockhash);
        }

        Ok(())
    }

    fn ensure_nonce_accounts(&mut self) -> Result<(), NonceError> {
        let mut modified = false;
        let target_count = self.config.nonce_accounts.len();

        for slot in 0..target_count {
            if self.nonce_accounts.iter().any(|entry| entry.index == slot) {
                continue;
            }

            let new_nonce = self.create_nonce_account()?;
            self.nonce_accounts.push(PubkeySlot {
                index: slot,
                pubkey: new_nonce,
            });
            modified = true;
        }

        self.nonce_accounts.sort_by_key(|entry| entry.index);
        if modified {
            self.persist_nonce_accounts()?;
        }
        Ok(())
    }

    fn create_nonce_account(&self) -> Result<Pubkey, NonceError> {
        let nonce_keypair = Keypair::new();
        let rent = self
            .rpc
            .get_minimum_balance_for_rent_exemption(NonceState::size())
            .map_err(NonceError::Rpc)?;

        let instructions = system_instruction::create_nonce_account(
            &self.operator.pubkey(),
            &nonce_keypair.pubkey(),
            &self.operator.pubkey(),
            rent,
        );

        let recent_blockhash = self.rpc.get_latest_blockhash().map_err(NonceError::Rpc)?;

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.operator.pubkey()),
            &[self.operator.as_ref(), &nonce_keypair],
            recent_blockhash,
        );

        self.rpc
            .send_and_confirm_transaction(&transaction)
            .map_err(NonceError::Rpc)?;

        Ok(nonce_keypair.pubkey())
    }

    /// Fetches nonce blockhash from RPC (used for cache refresh)
    fn fetch_nonce_blockhash_rpc(&self, nonce: &Pubkey) -> Result<Hash, NonceError> {
        let params = json!([
            nonce.to_string(),
            {
                "commitment": "processed",
                "encoding": "jsonParsed"
            }
        ]);
        let response: Value = self
            .rpc
            .send(RpcRequest::GetAccountInfo, params)
            .map_err(NonceError::Rpc)?;

        let value = response
            .get("value")
            .ok_or(NonceError::InvalidState(*nonce))?;
        if value.is_null() {
            return Err(NonceError::InvalidState(*nonce));
        }
        let info = value
            .get("data")
            .and_then(|data| data.get("parsed"))
            .and_then(|parsed| parsed.get("info"))
            .ok_or_else(|| NonceError::MissingField {
                account: *nonce,
                field: "parsed.info".to_string(),
            })?;
        let blockhash_str = info
            .get("blockhash")
            .and_then(Value::as_str)
            .ok_or_else(|| NonceError::MissingField {
                account: *nonce,
                field: "blockhash".to_string(),
            })?;
        Hash::from_str(blockhash_str).map_err(|e| NonceError::ParseBlockhash(e.to_string()))
    }

    /// Returns cached blockhash for a slot (no RPC call)
    fn get_cached_blockhash(&self, slot: usize) -> Option<Hash> {
        self.blockhash_cache.read().unwrap().get(&slot).copied()
    }

    /// Prepares a nonce from cache. Returns error only if slot doesn't exist.
    pub fn prepare_nonce(&self, slot: usize) -> Result<PreparedNonce, NonceError> {
        let entry = self
            .nonce_accounts
            .iter()
            .find(|entry| entry.index == slot)
            .ok_or(NonceError::MissingSlot(slot))?;

        // Use cached blockhash - no RPC call
        let blockhash = self
            .get_cached_blockhash(slot)
            .ok_or(NonceError::NonceCacheMiss(slot))?;

        Ok(PreparedNonce {
            account: entry.pubkey,
            blockhash,
        })
    }

    /// Creates an execution plan using cached nonce (no RPC call)
    pub fn plan_for_slot(
        &self,
        slot: usize,
        _compute_units: u32,
        compute_unit_price_micro_lamports: u64,
        tip_lamports: u64,
    ) -> Result<TxExecutionPlan, NonceError> {
        let nonce = self.prepare_nonce(slot)?;
        Ok(TxExecutionPlan {
            nonce,
            compute_unit_price_micro_lamports,
            tip_lamports,
        })
    }

    /// Marks a slot as in-flight (transaction sent, awaiting confirmation)
    pub fn mark_in_flight(&self, slot: usize) {
        self.in_flight.write().unwrap().insert(slot);
        debug!("Marked nonce slot {} as in-flight", slot);
    }

    /// Clears in-flight status for a slot
    pub fn clear_in_flight(&self, slot: usize) {
        self.in_flight.write().unwrap().remove(&slot);
        debug!("Cleared in-flight status for nonce slot {}", slot);
    }

    /// Checks if a slot is currently in-flight
    pub fn is_in_flight(&self, slot: usize) -> bool {
        self.in_flight.read().unwrap().contains(&slot)
    }

    /// Finds the next available slot that is not in-flight
    pub fn next_available_slot(&self, preferred: usize) -> Option<usize> {
        let in_flight = self.in_flight.read().unwrap();
        let total_slots = self.nonce_accounts.len();

        // Try preferred slot first
        if !in_flight.contains(&preferred) {
            return Some(preferred);
        }

        // Round-robin through other slots
        for offset in 1..total_slots {
            let candidate = (preferred + offset) % total_slots;
            if !in_flight.contains(&candidate) {
                return Some(candidate);
            }
        }

        // All slots are in-flight
        None
    }

    /// Refreshes a slot's cached blockhash from RPC.
    /// Called after a transaction using this slot is confirmed.
    /// Returns Ok even if RPC fails - the cache keeps the old value.
    pub fn refresh_slot_cache(&self, slot: usize) -> Result<(), NonceError> {
        let entry = self
            .nonce_accounts
            .iter()
            .find(|entry| entry.index == slot)
            .ok_or(NonceError::MissingSlot(slot))?;

        match self.fetch_nonce_blockhash_rpc(&entry.pubkey) {
            Ok(blockhash) => {
                self.blockhash_cache
                    .write()
                    .unwrap()
                    .insert(slot, blockhash);
                info!(
                    "Refreshed nonce cache | slot {} | account {} | new blockhash {}",
                    slot, entry.pubkey, blockhash
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed to refresh nonce slot {} cache (will retry later): {}",
                    slot, e
                );
                Err(e)
            }
        }
    }

    /// Advances a nonce account on-chain (forces blockhash change)
    pub fn advance_nonce(&self, nonce: &Pubkey) -> Result<Signature, NonceError> {
        let ix = system_instruction::advance_nonce_account(nonce, &self.operator.pubkey());
        let bh = self.rpc.get_latest_blockhash().map_err(NonceError::Rpc)?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.operator.pubkey()),
            &[self.operator.as_ref()],
            bh,
        );
        self.rpc
            .send_and_confirm_transaction(&tx)
            .map_err(NonceError::Rpc)
    }

    pub fn nonce_accounts(&self) -> &[PubkeySlot] {
        &self.nonce_accounts
    }

    /// Logs current nonce states (fetches from RPC)
    pub fn log_nonce_states(&self) {
        for entry in &self.nonce_accounts {
            match self.fetch_nonce_blockhash_rpc(&entry.pubkey) {
                Ok(hash) => info!(
                    "Nonce slot {} | Account {} | Blockhash {}",
                    entry.index, entry.pubkey, hash
                ),
                Err(err) => warn!(
                    "Failed to fetch nonce state for slot {} ({:?}): {}",
                    entry.index, entry.pubkey, err
                ),
            }
        }
    }

    /// Logs a single nonce state (fetches from RPC)
    pub fn log_nonce_state(&self, pubkey: &Pubkey) {
        match self.fetch_nonce_blockhash_rpc(pubkey) {
            Ok(hash) => info!(
                "Nonce refreshed | Account: {} | New blockhash: {}",
                pubkey, hash
            ),
            Err(err) => warn!("Failed to fetch nonce state for {}: {}", pubkey, err),
        }
    }

    fn persist_nonce_accounts(&self) -> Result<(), NonceError> {
        let mut updates = Vec::new();

        for entry in &self.nonce_accounts {
            let key = format!("NONCE_ACCOUNT_{}", entry.index + 1);
            updates.push((key, Some(entry.pubkey.to_string())));
        }

        write_env_updates(self.config.env_path.as_path(), &updates)?;
        Ok(())
    }
}

fn write_env_updates(path: &Path, updates: &[(String, Option<String>)]) -> Result<(), NonceError> {
    let mut existing_lines = if path.exists() {
        fs::read_to_string(path)
            .map_err(|e| NonceError::Io(path.into(), e))?
            .lines()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut processed: HashSet<String> = HashSet::new();
    for line in existing_lines.iter_mut() {
        if let Some((key, _)) = split_key_value(line) {
            if let Some((_, value)) = updates.iter().find(|(k, _)| k == key) {
                processed.insert(key.to_string());
                *line = format!("{key}={}", value.clone().unwrap_or_default());
            }
        }
    }

    for (key, value) in updates {
        if processed.contains(key) {
            continue;
        }
        existing_lines.push(format!("{key}={}", value.clone().unwrap_or_default()));
    }

    let tmp_path = path.with_extension("env.tmp");
    fs::write(&tmp_path, existing_lines.join("\n") + "\n")
        .map_err(|e| NonceError::Io(tmp_path.clone(), e))?;
    fs::rename(&tmp_path, path).map_err(|e| NonceError::Io(path.into(), e))?;
    Ok(())
}

fn split_key_value(line: &str) -> Option<(&str, &str)> {
    if line.trim_start().starts_with('#') || line.trim().is_empty() {
        return None;
    }
    let mut parts = line.splitn(2, '=');
    let key = parts.next()?.trim();
    let value = parts.next().unwrap_or("").trim();
    if key.is_empty() {
        None
    } else {
        Some((key, value))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NonceError {
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcClientError),
    #[error("invalid nonce account state for {0}")]
    InvalidState(Pubkey),
    #[error("nonce slot {0} is not configured")]
    MissingSlot(usize),
    #[error("nonce slot {0} not in cache (should never happen after startup)")]
    NonceCacheMiss(usize),
    #[error("all nonce slots are currently in-flight")]
    AllSlotsInFlight,
    #[error("io error for {0:?}")]
    Io(PathBuf, #[source] std::io::Error),
    #[error("nonce account {0} data is not json-parsed nonce")]
    InvalidEncoding(Pubkey),
    #[error("nonce account {account} missing field {field}")]
    MissingField { account: Pubkey, field: String },
    #[error("failed to parse nonce blockhash: {0}")]
    ParseBlockhash(String),
}
