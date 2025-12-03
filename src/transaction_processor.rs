use crate::lazy_signature::LazySignature;
use crate::parsers::{ParsedAction, ParserResult};
use crate::program_registry::ProgramRegistry;
use smallvec::SmallVec;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use std::cell::RefCell;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use yellowstone_grpc_proto::prelude::{
    InnerInstruction as ProtoInnerInstruction, SubscribeUpdateTransactionInfo, TokenBalance,
};

/// Represents a fully parsed transaction with all actions (instruction + event pairs)
#[derive(Debug, Clone)]
pub struct ParsedTransaction {
    pub signature: LazySignature,
    /// All parsed actions (instruction + event bundled together via merge pattern)
    pub actions: SmallVec<[ParsedAction; 4]>,
    pub token_balances: Vec<TokenBalanceChange>,
    pub compute_units_consumed: Option<u64>,
    pub total_parsing_time_us: u64,
    /// True if the transaction failed on-chain
    pub failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenBalanceKey {
    pub owner: Pubkey,
    pub mint: Pubkey,
}

#[derive(Debug, Clone)]
pub struct TokenBalanceChange {
    pub key: TokenBalanceKey,
    pub token_account: Option<Pubkey>,
    pub decimals: u8,
    pub pre_amount: Option<u64>,
    pub post_amount: Option<u64>,
}

// ============================================================================
// Performance Optimization: Thread-Local Account Key Cache
// ============================================================================
// Instead of allocating a new Vec<Pubkey> for every transaction, we reuse
// a thread-local buffer. This eliminates allocation overhead on the hot path.

/// Cache for reusing account key vectors across transactions
struct AccountKeyCache {
    keys: Vec<Pubkey>,
}

thread_local! {
    /// Thread-local cache for account keys - typical transaction has ~20-40 accounts
    static KEY_CACHE: RefCell<AccountKeyCache> = RefCell::new(AccountKeyCache {
        keys: Vec::with_capacity(64)
    });
}

pub struct TransactionProcessor {
    registry: Arc<ProgramRegistry>,
}

impl TransactionProcessor {
    pub fn new(registry: Arc<ProgramRegistry>) -> Self {
        Self { registry }
    }

    /// Convert Yellowstone Proto InnerInstruction to Solana SDK CompiledInstruction.
    ///
    /// The main difference is that Proto uses u32 for indices (to be safe),
    /// while Solana SDK CompiledInstruction uses u8.
    /// In practice, a transaction can't have > 255 accounts, so this cast is safe for valid txs.
    #[inline]
    fn convert_proto_to_compiled(proto: &ProtoInnerInstruction) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: proto.program_id_index as u8,
            accounts: proto.accounts.clone(),
            data: proto.data.clone(),
        }
    }

    /// Convert a Proto outer instruction to Solana SDK CompiledInstruction.
    #[inline]
    fn convert_outer_instruction(
        instruction: &yellowstone_grpc_proto::prelude::CompiledInstruction,
    ) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: instruction.program_id_index as u8,
            accounts: instruction.accounts.clone(),
            data: instruction.data.clone(),
        }
    }

    /// Process a transaction from the Yellowstone gRPC stream.
    ///
    /// Uses the "merge pattern": for each instruction (outer or inner), we immediately
    /// look for its associated event in the remaining inner instructions and bundle them together.
    pub fn process_yellowstone_transaction(
        &self,
        signature: &[u8],
        transaction: &SubscribeUpdateTransactionInfo,
    ) -> ParserResult<ParsedTransaction> {
        let start = Instant::now();

        // Use thread-local cache for account keys (avoids allocation per transaction)
        let (actions, token_balances) = self.with_account_keys(transaction, |account_keys| {
            let actions = self.process_all_instructions_merged(transaction, account_keys)?;
            let token_balances = self.extract_token_balance_changes(transaction, account_keys);
            Ok((actions, token_balances))
        })?;

        // Extract failed status and compute units from meta
        let meta = transaction.meta.as_ref();
        let failed = meta.map(|m| m.err.is_some()).unwrap_or(false);
        let compute_units_consumed = meta.and_then(|m| {
            if m.err.is_none() {
                m.compute_units_consumed
            } else {
                None
            }
        });

        let total_parsing_time_us = start.elapsed().as_micros() as u64;

        // Only format debug message if debug logging is enabled (lazy evaluation)
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "Transaction parsed in {}μs | Actions: {}",
                total_parsing_time_us,
                actions.len()
            );
        }

        Ok(ParsedTransaction {
            signature: LazySignature::new(signature.to_vec()),
            actions,
            token_balances,
            compute_units_consumed,
            total_parsing_time_us,
            failed,
        })
    }

    /// Extract account keys into thread-local cache and execute closure with the keys.
    /// This avoids allocating a new Vec<Pubkey> for every transaction.
    fn with_account_keys<F, R>(&self, transaction: &SubscribeUpdateTransactionInfo, f: F) -> R
    where
        F: FnOnce(&[Pubkey]) -> R,
    {
        KEY_CACHE.with(|cache_cell| {
            let mut cache = cache_cell.borrow_mut();
            cache.keys.clear(); // Reset length, keep capacity

            // Extract static account keys from the transaction message
            if let Some(tx) = &transaction.transaction {
                if let Some(message) = &tx.message {
                    for key_bytes in &message.account_keys {
                        if key_bytes.len() == 32 {
                            // Zero-copy conversion: directly interpret bytes as Pubkey
                            if let Ok(bytes) = <[u8; 32]>::try_from(key_bytes.as_slice()) {
                                cache.keys.push(Pubkey::from(bytes));
                            }
                        }
                    }
                }
            }

            // For v0 transactions, also add loaded addresses from meta
            if let Some(meta) = &transaction.meta {
                for key_bytes in &meta.loaded_writable_addresses {
                    if key_bytes.len() == 32 {
                        if let Ok(bytes) = <[u8; 32]>::try_from(key_bytes.as_slice()) {
                            cache.keys.push(Pubkey::from(bytes));
                        }
                    }
                }
                for key_bytes in &meta.loaded_readonly_addresses {
                    if key_bytes.len() == 32 {
                        if let Ok(bytes) = <[u8; 32]>::try_from(key_bytes.as_slice()) {
                            cache.keys.push(Pubkey::from(bytes));
                        }
                    }
                }
            }

            // Execute the closure with the cached keys
            f(&cache.keys)
        })
    }

    /// Process all instructions using the merge pattern.
    ///
    /// For each instruction (outer or inner), we:
    /// 1. Check if we have a parser for its program
    /// 2. Pass the instruction + remaining inner instructions to the parser
    /// 3. The parser returns the instruction bundled with its event (if found)
    fn process_all_instructions_merged(
        &self,
        transaction: &SubscribeUpdateTransactionInfo,
        account_keys: &[Pubkey],
    ) -> ParserResult<SmallVec<[ParsedAction; 4]>> {
        let mut results = SmallVec::new();

        let tx = match &transaction.transaction {
            Some(tx) => tx,
            None => return Ok(results),
        };

        let message = match &tx.message {
            Some(msg) => msg,
            None => return Ok(results),
        };

        // Iterate over top-level instructions
        for (idx, outer_instruction) in message.instructions.iter().enumerate() {
            // Bounds check
            if (outer_instruction.program_id_index as usize) >= account_keys.len() {
                continue;
            }

            // Get all inner instructions for this top-level index
            let inner_group = transaction
                .meta
                .as_ref()
                .and_then(|meta| meta.inner_instructions.iter().find(|i| i.index == idx as u32));

            // Convert Proto inner instructions to SDK CompiledInstructions
            let all_inner_ixs: Vec<CompiledInstruction> = match inner_group {
                Some(group) => group
                    .instructions
                    .iter()
                    .map(Self::convert_proto_to_compiled)
                    .collect(),
                None => Vec::new(),
            };

            // Convert outer instruction
            let outer_compiled = Self::convert_outer_instruction(outer_instruction);
            let outer_program_id = account_keys[outer_instruction.program_id_index as usize];

            // -------------------------------------------------------
            // SCENARIO A: Our program is the top-level instruction
            // -------------------------------------------------------
            if let Some(parser) = self.registry.get_parser(&outer_program_id) {
                if let Ok(Some(action)) = parser.parse_instruction_and_merge(
                    &outer_compiled,
                    account_keys,
                    &all_inner_ixs, // Search full inner list for event
                ) {
                    results.push(action);
                    // Continue to check inner instructions too (a top-level Pump Fun
                    // instruction might also have nested CPIs we want to parse)
                }
            }

            // -------------------------------------------------------
            // SCENARIO B: Our program is a nested CPI (e.g., Jupiter -> PumpFun)
            // -------------------------------------------------------
            // Iterate inner instructions looking for our program's instructions
            for (i, inner_ix) in all_inner_ixs.iter().enumerate() {
                // Bounds check
                if (inner_ix.program_id_index as usize) >= account_keys.len() {
                    continue;
                }

                let inner_program_id = account_keys[inner_ix.program_id_index as usize];

                if let Some(parser) = self.registry.get_parser(&inner_program_id) {
                    // CRITICAL: The context for this inner instruction is
                    // everything that came AFTER it in the flattened list (look-ahead only)
                    let remaining_context = &all_inner_ixs[(i + 1)..];

                    if let Ok(Some(action)) = parser.parse_instruction_and_merge(
                        inner_ix,
                        account_keys,
                        remaining_context,
                    ) {
                        results.push(action);
                    }
                }
            }
        }

        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Processed {} actions via merge pattern", results.len());
        }
        Ok(results)
    }

    fn extract_token_balance_changes(
        &self,
        transaction: &SubscribeUpdateTransactionInfo,
        account_keys: &[Pubkey],
    ) -> Vec<TokenBalanceChange> {
        let meta = match transaction.meta.as_ref() {
            Some(meta) => meta,
            None => return Vec::new(),
        };

        let mut balances: HashMap<TokenBalanceKey, TokenBalanceChange> =
            HashMap::with_capacity(meta.post_token_balances.len());

        for pre in &meta.pre_token_balances {
            if let Some(partial) = Self::parse_token_balance(pre, account_keys) {
                let entry = balances
                    .entry(partial.key)
                    .or_insert_with(|| TokenBalanceChange {
                        key: partial.key,
                        token_account: partial.token_account,
                        decimals: partial.decimals.unwrap_or(0),
                        pre_amount: None,
                        post_amount: None,
                    });

                entry.pre_amount = partial.amount;
                if entry.token_account.is_none() {
                    entry.token_account = partial.token_account;
                }
                if let Some(decimals) = partial.decimals {
                    entry.decimals = decimals;
                }
            }
        }

        for post in &meta.post_token_balances {
            if let Some(partial) = Self::parse_token_balance(post, account_keys) {
                let entry = balances
                    .entry(partial.key)
                    .or_insert_with(|| TokenBalanceChange {
                        key: partial.key,
                        token_account: partial.token_account,
                        decimals: partial.decimals.unwrap_or(0),
                        pre_amount: None,
                        post_amount: None,
                    });

                entry.post_amount = partial.amount;
                if entry.token_account.is_none() {
                    entry.token_account = partial.token_account;
                }
                if let Some(decimals) = partial.decimals {
                    entry.decimals = decimals;
                }
            }
        }

        balances.into_values().collect()
    }

    fn parse_token_balance(
        token_balance: &TokenBalance,
        account_keys: &[Pubkey],
    ) -> Option<PartialTokenBalance> {
        let owner = match Pubkey::from_str(token_balance.owner.as_str()) {
            Ok(owner) => owner,
            Err(e) => {
                log::debug!(
                    "Failed to parse token owner {}: {:?}",
                    token_balance.owner,
                    e
                );
                return None;
            }
        };
        let mint = match Pubkey::from_str(token_balance.mint.as_str()) {
            Ok(mint) => mint,
            Err(e) => {
                log::debug!("Failed to parse token mint {}: {:?}", token_balance.mint, e);
                return None;
            }
        };

        let token_account = account_keys
            .get(token_balance.account_index as usize)
            .copied();

        let (amount, decimals) = match token_balance.ui_token_amount.as_ref() {
            Some(ui) => {
                let decimals = ui.decimals.min(u32::from(u8::MAX)) as u8;
                match ui.amount.parse::<u64>() {
                    Ok(value) => (Some(value), Some(decimals)),
                    Err(e) => {
                        log::debug!("Failed to parse token amount {}: {:?}", ui.amount, e);
                        (None, Some(decimals))
                    }
                }
            }
            None => (None, None),
        };

        Some(PartialTokenBalance {
            key: TokenBalanceKey { owner, mint },
            token_account,
            decimals,
            amount,
        })
    }
}

struct PartialTokenBalance {
    key: TokenBalanceKey,
    token_account: Option<Pubkey>,
    decimals: Option<u8>,
    amount: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_processor_creation() {
        let registry = Arc::new(ProgramRegistry::new());
        let processor = TransactionProcessor::new(registry);

        // Basic smoke test - processor should be created without panic
        assert!(processor.registry.is_empty());
    }

    #[test]
    fn test_transaction_processor_with_defaults() {
        let registry = Arc::new(ProgramRegistry::with_defaults());
        let processor = TransactionProcessor::new(registry);

        // Should have default parsers registered
        assert!(!processor.registry.is_empty());
    }
}
