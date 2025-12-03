#![allow(dead_code)]

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, time::sleep};

use base64::{engine::general_purpose::STANDARD as BASE64_ENGINE, Engine as _};
use bs58;
use futures::future;
use log::{debug, info, warn};

use crate::info_async;
use rayon::prelude::*;
use serde_json::{json, Value};
use solana_sdk::{
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Signer, SignerError},
    transaction::VersionedTransaction,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::native_mint;
use thiserror::Error;
use yellowstone_grpc_proto::{
    prelude::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo},
    solana::storage::confirmed_block,
};

use crate::{
    config::{Config, TargetWalletConfig},
    executor::{ExecutionError, ExecutionPipeline, ProcessorEndpoint},
    nonce::{NonceError, NonceManager, PreparedNonce, TxExecutionPlan},
    parsers::{
        pump_amm::{PumpAmmEvent, PumpAmmInstruction, PumpAmmOperation},
        pump_fun::{PumpFunEvent, PumpFunInstruction, PumpFunOperation},
        ParsedAction,
    },
    swap::{
        common::{compute_pump_amm_price, compute_pump_fun_price},
        constants::{
            buy_quote_input_internal, get_buy_token_amount_from_sol_amount,
            get_sell_sol_amount_from_token_amount, sell_base_input_internal, Fees,
        },
        pump::{
            self, user_volume_accumulator_pda as pump_fun_volume_pda, BuyExactSolInAccounts,
            PumpFunBuyCoreInstructions, PumpFunBuyCoreRequest, PumpFunPreambleParams,
            PumpFunSellCoreInstructions, PumpFunSellCoreRequest, PumpFunTxBuilder,
            PumpTransactionBuilderError, SellAccounts as PumpFunSellAccounts,
        },
        pump_amm::{
            self, user_volume_accumulator_pda as pump_amm_volume_pda, BuyExactQuoteInAccounts,
            PumpAmmBuilderError, PumpAmmBuyCoreInstructions, PumpAmmBuyCoreRequest,
            PumpAmmPreambleParams, PumpAmmSellCoreInstructions, PumpAmmSellCoreRequest,
            PumpAmmTxBuilder, SellAccounts as PumpAmmSellAccounts,
        },
    },
    transaction_processor::{ParsedTransaction, TokenBalanceChange, TokenBalanceKey},
};

const DEFAULT_TOKEN_DECIMALS: u8 = 6;
const PUMP_FUN_CU_LIMIT: u32 = 121_000;
const PUMP_AMM_CU_LIMIT: u32 = 180_000;
const CONFIRMATION_TIMEOUT_SECS: u64 = 15;
const MIN_TARGET_SOL_LAMPORTS: u64 = 1000; // 0.000001 SOL
const NONCE_REFRESH_DELAY_MS: u64 = 700;
const DEAD_TRADE_WINDOW_SECS: u64 = 300;
const DEAD_TRADE_THRESHOLD_PCT: f64 = 3.0;

pub struct CopyTrader {
    config: Arc<Config>,
    execution: Option<Arc<CopyTradeRuntime>>,
    execution_enabled: bool,
    targets: HashMap<Pubkey, WalletTargetState>,
    position_index: PositionIndex,
    pump_fun: PumpFunTracker,
    pump_amm: PumpAmmTracker,
    operator: OperatorState,
    parsing_time_us: u64,
    grpc_dump: Option<Arc<GrpcDump>>,
    /// Cached Pump AMM pool state per base_mint for fallback sells
    pump_amm_pool_cache: HashMap<Pubkey, CachedPumpAmmPool>,
}

/// Cached pool state for Pump AMM fallback sells
#[derive(Clone)]
struct CachedPumpAmmPool {
    accounts: PumpAmmSellAccounts,
    base_reserve: u64,
    quote_reserve: u64,
    fees: Fees,
}

pub enum TraderMessage {
    ProcessTransaction {
        parsed_tx: Box<ParsedTransaction>,
        confirmation: Option<PendingConfirmation>,
        update: Option<SubscribeUpdateTransaction>,
    },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HandleResult {
    pub target_event_mismatch: bool,
}

impl CopyTrader {
    pub async fn run(mut self, mut receiver: mpsc::Receiver<TraderMessage>) {
        while let Some(message) = receiver.recv().await {
            match message {
                TraderMessage::ProcessTransaction {
                    parsed_tx,
                    confirmation,
                    update,
                } => {
                    if let Some(ref pending) = confirmation {
                        self.handle_operator_confirmation(pending, &parsed_tx);
                    }
                    let handle_result = self.handle_parsed_transaction(&parsed_tx);
                    if handle_result.target_event_mismatch {
                        if let (Some(dump), Some(update)) = (self.grpc_dump.as_ref(), update) {
                            if let Err(err) = dump.dump_failed_transaction(&update, &parsed_tx) {
                                warn!("Failed to dump gRPC transaction: {err:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn set_execution_enabled(&mut self, enabled: bool) {
        self.execution_enabled = enabled && self.execution.is_some();
    }

    pub fn new(
        config: Arc<Config>,
        execution: Option<Arc<CopyTradeRuntime>>,
        grpc_dump: Option<Arc<GrpcDump>>,
    ) -> Self {
        let mut targets = HashMap::new();
        let target_configs = config.target_wallets.clone();
        for target in target_configs {
            targets.insert(target.wallet, WalletTargetState::new(target));
        }

        let operator_wallet = config.operator_pubkey();
        Self {
            config,
            execution_enabled: execution.is_some(),
            execution,
            targets,
            position_index: PositionIndex::default(),
            pump_fun: PumpFunTracker::new(),
            pump_amm: PumpAmmTracker::new(),
            operator: OperatorState::new(operator_wallet),
            parsing_time_us: 0,
            grpc_dump,
            pump_amm_pool_cache: HashMap::new(),
        }
    }

    pub fn handle_parsed_transaction(&mut self, parsed_tx: &ParsedTransaction) -> HandleResult {
        self.parsing_time_us = parsed_tx.total_parsing_time_us;
        let balance_index = TokenBalanceIndex::new(&parsed_tx.token_balances);
        let mut result = HandleResult::default();

        for action in &parsed_tx.actions {
            if self.handle_action_with_balances(action, &balance_index, parsed_tx.compute_units_consumed)
            {
                result.target_event_mismatch = true;
            }
        }
        result
    }

    pub fn handle_operator_transaction(&mut self, parsed_tx: &ParsedTransaction) {
        if parsed_tx.actions.is_empty() {
            return;
        }
        let balance_index = TokenBalanceIndex::new(&parsed_tx.token_balances);
        for action in &parsed_tx.actions {
            self.update_operator_from_action(action, &balance_index);
        }
    }

    pub fn handle_operator_confirmation(
        &mut self,
        pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) {
        // Check for failed Pump Fun tx that needs AMM fallback
        if parsed_tx.failed && matches!(pending.protocol, TradeProtocol::PumpFun) {
            self.handle_failed_pump_fun_sell(pending);
        }

        if !parsed_tx.failed {
            self.handle_operator_transaction(parsed_tx);
            self.apply_operator_fill(pending, parsed_tx);
            self.update_tp_sl_position_after_confirmation(pending, parsed_tx);
            self.cleanup_tp_sl_after_confirmation(pending, parsed_tx);
        }

        if let Some(runtime) = self.execution.as_ref() {
            runtime.handle_confirmed_transaction(pending);
        }
    }

    /// Handle a failed Pump Fun sell by attempting a Pump AMM fallback
    fn handle_failed_pump_fun_sell(&mut self, pending: &PendingConfirmation) {
        let mint = &pending.mint;

        // Check if position has migrated to PumpAmm
        let Some(target) = self.targets.get_mut(&pending.target) else {
            return;
        };
        let Some(position) = target.tp_sl_positions.get_mut(mint) else {
            return;
        };

        // Only fallback if position was already migrated to PumpAmm
        if !matches!(position.protocol, TradeProtocol::PumpAmm) {
            warn!(
                "Pump Fun sell failed for {} but position not migrated to AMM",
                mint
            );
            position.pending_sell = false;
            return;
        }

        // Get cached pool state
        let Some(cached_pool) = self.pump_amm_pool_cache.get(mint).cloned() else {
            warn!(
                "Pump Fun sell failed for {} (migrated) but no AMM pool state cached - cannot fallback",
                mint
            );
            position.pending_sell = false;
            return;
        };

        // Get operator token balance
        let operator_tokens = self.operator.total_tokens(mint);
        if operator_tokens == 0 {
            info!(
                "Pump Fun sell failed for {} but operator has no tokens - skipping fallback",
                mint
            );
            position.pending_sell = false;
            return;
        }

        info!(
            "Transaction likely failed due to migrated bonding curve, sending Pump AMM Sell tx for {}",
            mint
        );

        // Build fallback Pump AMM sell
        let operator = self.config.operator_pubkey();
        let mut accounts = cached_pool.accounts.clone();
        retarget_pump_amm_sell_accounts(&mut accounts, &operator);

        let slippage_bps = slippage_pct_to_bps(target.config.slippage_pct) as u64;
        let quote_result = match sell_base_input_internal(
            operator_tokens,
            slippage_bps,
            cached_pool.base_reserve,
            cached_pool.quote_reserve,
            cached_pool.fees,
        ) {
            Ok(result) => result,
            Err(err) => {
                warn!("Pump AMM fallback math failed for {}: {}", mint, err);
                position.pending_sell = false;
                return;
            }
        };

        let payload = PumpAmmSellPayload {
            accounts,
            base_amount_in: operator_tokens,
            base_reserve: cached_pool.base_reserve,
            quote_reserve: cached_pool.quote_reserve,
            fees: cached_pool.fees,
            expected_sol_out: quote_result.ui_quote,
        };

        let signal = ExecutionSignal::pump_amm_sell(
            target.config.clone(),
            0, // slot not relevant for fallback
            quote_result.ui_quote,
            operator_tokens,
            ExecutionPayload::PumpAmmSell(payload),
            self.parsing_time_us,
            None,
        );

        // Keep pending_sell = true since we're sending another tx
        self.emit_execution_signal(signal);
    }

    fn update_tp_sl_position_after_confirmation(
        &mut self,
        pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) {
        let should_track = self
            .targets
            .get(&pending.target)
            .map(|state| !state.config.mirror_sells)
            .unwrap_or(false);
        if !should_track {
            return;
        }
        let buy_info = match pending.protocol {
            TradeProtocol::PumpFun => self.extract_pump_fun_buy(pending, parsed_tx),
            TradeProtocol::PumpAmm => self.extract_pump_amm_buy(pending, parsed_tx),
        };
        let Some((buy_price, tokens_bought)) = buy_info else {
            return;
        };
        let operator_total_after = self.operator.total_tokens(&pending.mint);
        if let Some(target) = self.targets.get_mut(&pending.target) {
            let inserted = CopyTrader::upsert_tp_sl_position(
                target,
                pending.mint,
                buy_price,
                tokens_bought,
                pending.protocol,
                operator_total_after,
            );
            if inserted {
                self.position_index
                    .insert(pending.mint, target.config.wallet);
            }
        }
    }

    fn upsert_tp_sl_position(
        target: &mut WalletTargetState,
        mint: Pubkey,
        buy_price: f64,
        tokens_bought: u64,
        protocol: TradeProtocol,
        operator_total_after: u64,
    ) -> bool {
        if buy_price <= 0.0 {
            return false;
        }
        let previous_tokens = operator_total_after.saturating_sub(tokens_bought);
        let take_profit_pct = target.config.take_profit_pct;
        let stop_loss_pct = target.config.stop_loss_pct;
        let mut was_new = false;
        let entry = match target.tp_sl_positions.entry(mint) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(vacant) => {
                was_new = true;
                vacant.insert(TpSlPosition::new(
                    protocol,
                    buy_price,
                    take_profit_pct,
                    stop_loss_pct,
                ))
            }
        };
        if previous_tokens == 0 {
            entry.buy_price = buy_price;
        } else {
            entry.record_additional_buy(previous_tokens, tokens_bought, buy_price);
        }
        entry.pending_sell = false;
        entry.last_log_at = Instant::now();
        log_tp_sl_targets(
            target.config.wallet,
            mint,
            entry.buy_price,
            entry.tp_price(),
            entry.sl_price(),
        );
        was_new
    }

    fn cleanup_tp_sl_after_confirmation(
        &mut self,
        pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) {
        let Some(target) = self.targets.get_mut(&pending.target) else {
            return;
        };
        let operator = self.config.operator_pubkey();
        let sold = match pending.protocol {
            TradeProtocol::PumpFun => parsed_tx.actions.iter().any(|action| {
                if let ParsedAction::PumpFun(op) = action {
                    if let Some(PumpFunEvent::TradeEvent { is_buy: false, user, mint, .. }) = &op.event {
                        return user == &operator && mint == &pending.mint;
                    }
                }
                false
            }),
            TradeProtocol::PumpAmm => parsed_tx.actions.iter().any(|action| {
                if let ParsedAction::PumpAmm(op) = action {
                    if let Some(PumpAmmEvent::SellEvent { user, .. }) = &op.event {
                        return user == &operator;
                    }
                }
                false
            }),
        };
        if sold || self.operator.total_tokens(&pending.mint) == 0 {
            if target.tp_sl_positions.remove(&pending.mint).is_some() {
                self.position_index
                    .remove(&pending.mint, &target.config.wallet);
            }
        }
    }

    fn extract_pump_fun_buy(
        &self,
        pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) -> Option<(f64, u64)> {
        let operator = self.config.operator_pubkey();
        parsed_tx.actions.iter().find_map(|action| {
            if let ParsedAction::PumpFun(op) = action {
                if let Some(PumpFunEvent::TradeEvent {
                    is_buy,
                    user,
                    mint,
                    virtual_sol_reserves,
                    virtual_token_reserves,
                    token_amount,
                    ..
                }) = &op.event
                {
                    if !is_buy || user != &operator || mint != &pending.mint {
                        return None;
                    }
                    return compute_pump_fun_price(*virtual_sol_reserves, *virtual_token_reserves)
                        .map(|price| (price, *token_amount));
                }
            }
            None
        })
    }

    fn extract_pump_amm_buy(
        &self,
        _pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) -> Option<(f64, u64)> {
        let operator = self.config.operator_pubkey();
        parsed_tx.actions.iter().find_map(|action| {
            if let ParsedAction::PumpAmm(op) = action {
                if let Some(PumpAmmEvent::BuyEvent {
                    user,
                    pool_base_token_reserves,
                    pool_quote_token_reserves,
                    base_amount_out,
                    ..
                }) = &op.event
                {
                    if user == &operator {
                        let price = compute_pump_amm_price(
                            (*pool_base_token_reserves).into(),
                            (*pool_quote_token_reserves).into(),
                        );
                        return Some((price, *base_amount_out));
                    }
                }
            }
            None
        })
    }

    /// Process TP/SL triggers from a PumpFunOperation (merged instruction + event).
    fn process_pump_fun_tp_sl_from_action(&mut self, op: &PumpFunOperation) -> bool {
        let event = match &op.event {
            Some(ev) => ev,
            None => return false,
        };
        
        let PumpFunEvent::TradeEvent {
            mint,
            virtual_sol_reserves,
            virtual_token_reserves,
            ..
        } = event;
        
        let Some(price) = compute_pump_fun_price(*virtual_sol_reserves, *virtual_token_reserves)
        else {
            return false;
        };
        if price <= 0.0 {
            return false;
        }
        let slot = event_slot(event);
        let operator = self.config.operator_pubkey();
        let mut mismatch = false;
        let mut actions: Vec<TpSlAction> = Vec::new();
        let target_wallets = self.position_index.targets_for(mint);
        if target_wallets.is_empty() {
            return mismatch;
        }
        for wallet in target_wallets {
            let Some(target) = self.targets.get_mut(&wallet) else {
                self.position_index.remove(mint, &wallet);
                continue;
            };
            if target.config.mirror_sells {
                continue;
            }
            if let Some(position) = target.tp_sl_positions.get_mut(mint) {
                // Skip Pump Fun events if this position has migrated to Pump AMM
                if matches!(position.protocol, TradeProtocol::PumpAmm) {
                    continue;
                }
                let pct_change = position.percentage_change(price);
                if position.should_log() {
                    log_tp_sl_status(target.config.wallet, *mint, position, price, pct_change);
                    position.mark_logged();
                }
                if position.pending_sell {
                    continue;
                }
                let mut exit_trigger = None;
                if position.should_force_exit(
                    price,
                    DEAD_TRADE_THRESHOLD_PCT,
                    Duration::from_secs(DEAD_TRADE_WINDOW_SECS),
                ) {
                    exit_trigger = Some(TpSlTrigger::Stagnation);
                } else if let Some(trigger) = position.evaluate_trigger(pct_change) {
                    exit_trigger = Some(trigger);
                }
                if let Some(trigger) = exit_trigger {
                    let operator_tokens = self.operator.total_tokens(mint);
                    if operator_tokens == 0 {
                        if target.tp_sl_positions.remove(mint).is_some() {
                            self.position_index.remove(mint, &target.config.wallet);
                        }
                        continue;
                    }
                    // Extract accounts from the operation's instruction
                    let Some(mut accounts) = pump_fun_sell_accounts_from_operation(op) else {
                        mismatch = true;
                        continue;
                    };
                    retarget_pump_fun_sell_accounts(&mut accounts, &operator);
                    let payload = PumpFunSellPayload {
                        accounts,
                        amount: operator_tokens,
                        virtual_token_reserves: (*virtual_token_reserves).into(),
                        virtual_sol_reserves: (*virtual_sol_reserves).into(),
                    };
                    let expected_sol = get_sell_sol_amount_from_token_amount(
                        (*virtual_token_reserves).into(),
                        (*virtual_sol_reserves).into(),
                        operator_tokens,
                    );
                    let signal = ExecutionSignal::pump_fun_sell(
                        target.config.clone(),
                        slot,
                        expected_sol,
                        operator_tokens,
                        ExecutionPayload::PumpFunSell(payload),
                        self.parsing_time_us,
                        None,
                    );
                    position.pending_sell = true;
                    position.mark_logged();
                    actions.push(TpSlAction {
                        signal,
                        target_wallet: target.config.wallet,
                        mint: *mint,
                        trigger,
                        current_price: price,
                        pct_change,
                    });
                }
            }
        }
        for action in actions {
            log_tp_sl_trigger(
                action.target_wallet,
                action.mint,
                action.trigger,
                action.current_price,
                action.pct_change,
            );
            self.emit_execution_signal(action.signal);
        }
        mismatch
    }

    /// Process TP/SL triggers from a PumpAmmOperation (merged instruction + event).
    fn process_pump_amm_tp_sl_from_action(&mut self, op: &PumpAmmOperation) -> bool {
        let event = match &op.event {
            Some(ev) => ev,
            None => return false,
        };
        
        let slot = event_slot(event);
        let (base_reserve_u64, quote_reserve_u64, lp_fee_bps, protocol_fee_bps, coin_fee_bps) =
            match event {
                PumpAmmEvent::BuyEvent {
                    pool_base_token_reserves,
                    pool_quote_token_reserves,
                    lp_fee_basis_points,
                    protocol_fee_basis_points,
                    coin_creator_fee_basis_points,
                    ..
                }
                | PumpAmmEvent::SellEvent {
                    pool_base_token_reserves,
                    pool_quote_token_reserves,
                    lp_fee_basis_points,
                    protocol_fee_basis_points,
                    coin_creator_fee_basis_points,
                    ..
                } => (
                    *pool_base_token_reserves,
                    *pool_quote_token_reserves,
                    *lp_fee_basis_points,
                    *protocol_fee_basis_points,
                    *coin_creator_fee_basis_points,
                ),
            };
        if base_reserve_u64 == 0 || quote_reserve_u64 == 0 {
            return false;
        }
        let price = compute_pump_amm_price(base_reserve_u64 as u128, quote_reserve_u64 as u128);
        if price <= 0.0 {
            return false;
        }
        // Extract accounts from the operation's instruction
        let Some((base_accounts, base_mint)) = pump_amm_sell_accounts_from_operation(op) else {
            return true;
        };
        let fees = Fees {
            lp_fee_bps: (lp_fee_bps.min(u16::MAX as u64)) as u16,
            protocol_fee_bps: (protocol_fee_bps.min(u16::MAX as u64)) as u16,
            coin_creator_fee_bps: (coin_fee_bps.min(u16::MAX as u64)) as u16,
        };

        // Cache pool state for potential fallback sells
        self.pump_amm_pool_cache.insert(
            base_mint,
            CachedPumpAmmPool {
                accounts: base_accounts.clone(),
                base_reserve: base_reserve_u64,
                quote_reserve: quote_reserve_u64,
                fees,
            },
        );

        let operator = self.config.operator_pubkey();
        let mut mismatch = false;
        let mut actions: Vec<TpSlAction> = Vec::new();
        let target_wallets = self.position_index.targets_for(&base_mint);
        if target_wallets.is_empty() {
            return mismatch;
        }
        for wallet in target_wallets {
            let Some(target) = self.targets.get_mut(&wallet) else {
                self.position_index.remove(&base_mint, &wallet);
                continue;
            };
            if target.config.mirror_sells {
                continue;
            }
            if let Some(position) = target.tp_sl_positions.get_mut(&base_mint) {
                // Detect migration: if position was PumpFun, switch to PumpAmm
                if matches!(position.protocol, TradeProtocol::PumpFun) {
                    info!(
                        "TP/SL | Migration detected for {} | switching from PumpFun to PumpAmm",
                        base_mint
                    );
                    position.protocol = TradeProtocol::PumpAmm;
                }
                let pct_change = position.percentage_change(price);
                if position.should_log() {
                    log_tp_sl_status(target.config.wallet, base_mint, position, price, pct_change);
                    position.mark_logged();
                }
                if position.pending_sell {
                    continue;
                }
                let mut exit_trigger = None;
                if position.should_force_exit(
                    price,
                    DEAD_TRADE_THRESHOLD_PCT,
                    Duration::from_secs(DEAD_TRADE_WINDOW_SECS),
                ) {
                    exit_trigger = Some(TpSlTrigger::Stagnation);
                } else if let Some(trigger) = position.evaluate_trigger(pct_change) {
                    exit_trigger = Some(trigger);
                }
                if let Some(trigger) = exit_trigger {
                    let operator_tokens = self.operator.total_tokens(&base_mint);
                    if operator_tokens == 0 {
                        if target.tp_sl_positions.remove(&base_mint).is_some() {
                            self.position_index
                                .remove(&base_mint, &target.config.wallet);
                        }
                        continue;
                    }
                    let mut accounts = base_accounts.clone();
                    retarget_pump_amm_sell_accounts(&mut accounts, &operator);
                    let slippage_bps = slippage_pct_to_bps(target.config.slippage_pct) as u64;
                    let quote_result = match sell_base_input_internal(
                        operator_tokens,
                        slippage_bps,
                        base_reserve_u64,
                        quote_reserve_u64,
                        fees,
                    ) {
                        Ok(result) => result,
                        Err(err) => {
                            warn!("Pump AMM TP/SL math failed for mint {}: {err}", base_mint);
                            mismatch = true;
                            continue;
                        }
                    };
                    let payload = PumpAmmSellPayload {
                        accounts,
                        base_amount_in: operator_tokens,
                        base_reserve: base_reserve_u64,
                        quote_reserve: quote_reserve_u64,
                        fees,
                        expected_sol_out: quote_result.ui_quote,
                    };
                    let signal = ExecutionSignal::pump_amm_sell(
                        target.config.clone(),
                        slot,
                        quote_result.ui_quote,
                        operator_tokens,
                        ExecutionPayload::PumpAmmSell(payload),
                        self.parsing_time_us,
                        None,
                    );
                    position.pending_sell = true;
                    position.mark_logged();
                    actions.push(TpSlAction {
                        signal,
                        target_wallet: target.config.wallet,
                        mint: base_mint,
                        trigger,
                        current_price: price,
                        pct_change,
                    });
                }
            }
        }
        for action in actions {
            log_tp_sl_trigger(
                action.target_wallet,
                action.mint,
                action.trigger,
                action.current_price,
                action.pct_change,
            );
            self.emit_execution_signal(action.signal);
        }
        mismatch
    }

    fn emit_execution_signal(&self, signal: ExecutionSignal) {
        if !self.execution_enabled {
            return;
        }
        let runtime = match self.execution.as_ref() {
            Some(runtime) => Arc::clone(runtime),
            None => return,
        };

        tokio::spawn(async move {
            if let Err(err) = runtime.execute(signal).await {
                warn!("Execution pipeline failed: {err}");
            }
        });
    }

    fn handle_action_with_balances(
        &mut self,
        action: &ParsedAction,
        balances: &TokenBalanceIndex<'_>,
        compute_units_consumed: Option<u64>,
    ) -> bool {
        match action {
            ParsedAction::PumpFun(op) => {
                // Handle instruction for migration detection
                self.handle_pump_fun_instruction(&op.instruction);

                // Handle event if present
                if let Some(ev) = &op.event {
                    self.pump_fun.handle_event(ev);
                    let tp_sl_mismatch = self.process_pump_fun_tp_sl_from_action(op);
                    let target_mismatch = self.update_pump_fun_targets_from_action(
                        op,
                        balances,
                        compute_units_consumed,
                    );
                    return tp_sl_mismatch || target_mismatch;
                }
                false
            }
            ParsedAction::PumpAmm(op) => {
                // Handle event if present
                if let Some(ev) = &op.event {
                    self.pump_amm.handle_event(ev);
                    let tp_sl_mismatch = self.process_pump_amm_tp_sl_from_action(op);
                    let target_mismatch = self.update_pump_amm_targets_from_action(
                        op,
                        balances,
                        compute_units_consumed,
                    );
                    return tp_sl_mismatch || target_mismatch;
                }
                false
            }
        }
    }

    fn update_operator_from_action(
        &mut self,
        action: &ParsedAction,
        balances: &TokenBalanceIndex<'_>,
    ) {
        match action {
            ParsedAction::PumpFun(op) => {
                if let Some(PumpFunEvent::TradeEvent {
                    user,
                    mint,
                    sol_amount,
                    token_amount,
                    ..
                }) = &op.event
                {
                    if user != &self.operator.wallet {
                        return;
                    }
                    let slot = 0; // Events no longer have slot
                    Self::apply_base_token_update(
                        &mut self.operator.pump_fun,
                        balances.by_owner_mint(user, mint),
                        slot,
                        Some(*token_amount),
                        Some(*mint),
                    );
                    Self::apply_quote_update(
                        &mut self.operator.pump_fun,
                        balances.native_balance_for_owner(user),
                        slot,
                        Some(*sol_amount),
                    );
                }
            }
            ParsedAction::PumpAmm(op) => {
                match &op.event {
                    Some(PumpAmmEvent::BuyEvent {
                        user,
                        user_base_token_account,
                        user_quote_token_account,
                        base_amount_out,
                        user_quote_amount_in,
                        ..
                    }) => {
                        if user != &self.operator.wallet {
                            return;
                        }
                        let slot = 0;
                        Self::apply_base_token_update(
                            &mut self.operator.pump_amm,
                            balances.by_token_account(user_base_token_account),
                            slot,
                            Some(*base_amount_out),
                            None,
                        );
                        Self::apply_quote_update(
                            &mut self.operator.pump_amm,
                            balances.by_token_account(user_quote_token_account),
                            slot,
                            Some(*user_quote_amount_in),
                        );
                    }
                    Some(PumpAmmEvent::SellEvent {
                        user,
                        user_base_token_account,
                        user_quote_token_account,
                        base_amount_in,
                        user_quote_amount_out,
                        ..
                    }) => {
                        if user != &self.operator.wallet {
                            return;
                        }
                        let slot = 0;
                        Self::apply_base_token_update(
                            &mut self.operator.pump_amm,
                            balances.by_token_account(user_base_token_account),
                            slot,
                            Some(*base_amount_in),
                            None,
                        );
                        Self::apply_quote_update(
                            &mut self.operator.pump_amm,
                            balances.by_token_account(user_quote_token_account),
                            slot,
                            Some(*user_quote_amount_out),
                        );
                    }
                    None => {}
                }
            }
        }
    }

    fn handle_pump_fun_instruction(&mut self, instruction: &PumpFunInstruction) {
        self.pump_fun.handle_instruction(instruction);
        // Note: Migration detection would need a Migrate variant in the new parser
        // which doesn't exist yet in the simplified parser
    }

    /// When we see a Pump Fun migrate instruction, flip any TP/SL positions and cache pool state
    fn handle_pump_fun_migration(
        &mut self,
        mint: &Pubkey,
        pool: &Pubkey,
        pool_base_token_account: &Pubkey,
        pool_quote_token_account: &Pubkey,
        base_token_program: &Pubkey,
        coin_creator_vault_authority: &Pubkey,
    ) {
        let wallets = self.position_index.targets_for(mint);
        if wallets.is_empty() {
            return;
        }

        // Build and cache the pool state for potential fallback sells
        let accounts = self.build_migration_amm_accounts(
            mint,
            pool,
            pool_base_token_account,
            pool_quote_token_account,
            base_token_program,
            coin_creator_vault_authority,
        );

        // Default reserves right after migration (before any swaps)
        const DEFAULT_BASE_RESERVE: u64 = 206_900_000_000_000;
        const DEFAULT_QUOTE_RESERVE: u64 = 84_990_359_679;
        // Default fees (30 bps LP, 5 bps protocol, 5 bps creator)
        let fees = Fees {
            lp_fee_bps: 30,
            protocol_fee_bps: 5,
            coin_creator_fee_bps: 5,
        };

        self.pump_amm_pool_cache.insert(
            *mint,
            CachedPumpAmmPool {
                accounts,
                base_reserve: DEFAULT_BASE_RESERVE,
                quote_reserve: DEFAULT_QUOTE_RESERVE,
                fees,
            },
        );
        info!(
            "Cached Pump AMM pool state for {} from migrate instruction",
            mint
        );

        for wallet in wallets {
            let Some(target) = self.targets.get_mut(&wallet) else {
                continue;
            };
            if let Some(position) = target.tp_sl_positions.get_mut(mint) {
                if matches!(position.protocol, TradeProtocol::PumpFun) {
                    info!(
                        "TP/SL | Migration instruction detected for {} | switching from PumpFun to PumpAmm",
                        mint
                    );
                    position.protocol = TradeProtocol::PumpAmm;
                }
            }
        }
    }

    /// Build PumpAmmSellAccounts from migrate instruction accounts
    fn build_migration_amm_accounts(
        &self,
        mint: &Pubkey,
        pool: &Pubkey,
        pool_base_token_account: &Pubkey,
        pool_quote_token_account: &Pubkey,
        base_token_program: &Pubkey,
        coin_creator_vault_authority: &Pubkey,
    ) -> PumpAmmSellAccounts {
        use solana_sdk::pubkey;

        // Well-known constants for Pump AMM
        const PUMP_AMM_PROGRAM: Pubkey = pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
        const PUMP_AMM_GLOBAL_CONFIG: Pubkey =
            pubkey!("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw");
        const PUMP_AMM_FEE_CONFIG: Pubkey =
            pubkey!("G7HUCB9bL9QJAuvmkNB4tSiEdEgFirqGqPwLvKzWXSC7");
        const PUMP_AMM_FEE_PROGRAM: Pubkey =
            pubkey!("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
        const WSOL_MINT: Pubkey = pubkey!("So11111111111111111111111111111111111111112");
        const PROTOCOL_FEE_RECIPIENT: Pubkey =
            pubkey!("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");

        let operator = self.config.operator_pubkey();

        // Derive event authority PDA
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &PUMP_AMM_PROGRAM);

        // Derive coin creator vault ATA
        let coin_creator_vault_ata = get_associated_token_address_with_program_id(
            coin_creator_vault_authority,
            &WSOL_MINT,
            &spl_token::id(),
        );

        // Derive protocol fee recipient token account
        let protocol_fee_recipient_token_account = get_associated_token_address_with_program_id(
            &PROTOCOL_FEE_RECIPIENT,
            &WSOL_MINT,
            &spl_token::id(),
        );

        PumpAmmSellAccounts {
            pool: *pool,
            user: operator,
            global_config: PUMP_AMM_GLOBAL_CONFIG,
            base_mint: *mint,
            quote_mint: WSOL_MINT,
            user_base_token_account: get_associated_token_address_with_program_id(
                &operator,
                mint,
                base_token_program,
            ),
            user_quote_token_account: get_associated_token_address_with_program_id(
                &operator,
                &WSOL_MINT,
                &spl_token::id(),
            ),
            pool_base_token_account: *pool_base_token_account,
            pool_quote_token_account: *pool_quote_token_account,
            protocol_fee_recipient: PROTOCOL_FEE_RECIPIENT,
            protocol_fee_recipient_token_account,
            base_token_program: *base_token_program,
            quote_token_program: spl_token::id(),
            system_program: solana_sdk::system_program::id(),
            associated_token_program: spl_associated_token_account::id(),
            event_authority,
            program: PUMP_AMM_PROGRAM,
            coin_creator_vault_ata,
            coin_creator_vault_authority: *coin_creator_vault_authority,
            fee_config: PUMP_AMM_FEE_CONFIG,
            fee_program: PUMP_AMM_FEE_PROGRAM,
        }
    }

    pub fn is_target_wallet(&self, wallet: &Pubkey) -> bool {
        self.targets.contains_key(wallet)
    }

    pub fn wallet_snapshot(&self, wallet: &Pubkey) -> Option<WalletSnapshot> {
        self.targets.get(wallet).map(|state| state.snapshot())
    }

    pub fn pump_fun_snapshot(&self, wallet: &Pubkey) -> Option<PumpFunSnapshot> {
        self.pump_fun
            .holdings
            .get(wallet)
            .map(|state| state.snapshot())
    }

    pub fn pump_amm_snapshot(&self, wallet: &Pubkey) -> Option<PumpAmmSnapshot> {
        self.pump_amm
            .holdings
            .get(wallet)
            .map(|state| state.snapshot())
    }

    /// Update target wallet state and potentially emit trade signals from a PumpFunOperation.
    fn update_pump_fun_targets_from_action(
        &mut self,
        op: &PumpFunOperation,
        balances: &TokenBalanceIndex<'_>,
        compute_units_consumed: Option<u64>,
    ) -> bool {
        let event = match &op.event {
            Some(ev) => ev,
            None => return false,
        };
        
        let PumpFunEvent::TradeEvent {
            user,
            mint,
            sol_amount,
            token_amount,
            is_buy,
            virtual_token_reserves,
            virtual_sol_reserves,
            real_token_reserves,
            ..
        } = event;

        let Some(target) = self.targets.get_mut(user) else {
            debug!(
                "PumpFun trade from non-target wallet {} (mint {})",
                user, mint
            );
            return false;
        };

        if *is_buy {
            info!(
                "Target {} bought {:.4} tokens for {:.4} SOL (mint {})",
                user,
                tokens_to_amount(*token_amount),
                lamports_to_sol(*sol_amount),
                mint
            );
        }

        let slot = event_slot(event);
        Self::apply_base_token_update(
            &mut target.pump_fun,
            balances.by_owner_mint(user, mint),
            slot,
            Some(*token_amount),
            Some(*mint),
        );
        Self::apply_quote_update(
            &mut target.pump_fun,
            balances.native_balance_for_owner(user),
            slot,
            Some(*sol_amount),
        );

        // Build payload from the merged operation
        let payload = match build_pump_fun_payload_from_operation(op) {
            Some(payload) => payload,
            None => return true,
        };

        match (*is_buy, payload) {
            (true, ExecutionPayload::PumpFunBuy(mut buy_payload)) => {
                if *sol_amount < MIN_TARGET_SOL_LAMPORTS {
                    debug!(
                        "Skipping PumpFun buy mirror for {} due to small spend {} lamports",
                        user, sol_amount
                    );
                    return false;
                }
                let mut spend = self.config.buy_amount_lamports();
                if spend == 0 {
                    spend = *sol_amount;
                    debug!(
                        "BUY_AMOUNT_SOL not configured; mirroring target spend {} lamports",
                        spend
                    );
                }
                if spend == 0 {
                    return false;
                }
                target
                    .pump_fun_positions
                    .record_target_buy(*mint, *token_amount);
                if target.config.mirror_sells {
                    target
                        .mirror_positions
                        .record_target_buy(*mint, *token_amount);
                }
                let operator_tokens = get_buy_token_amount_from_sol_amount(
                    (*virtual_token_reserves).into(),
                    (*virtual_sol_reserves).into(),
                    (*real_token_reserves).into(),
                    spend,
                );
                if operator_tokens == 0 {
                    return false;
                }
                info!(
                    "Buying {:.4} tokens for {:.4} SOL (mint {}, target {})",
                    tokens_to_amount(operator_tokens),
                    lamports_to_sol(spend),
                    mint,
                    user
                );
                buy_payload.spendable_sol_in = spend;
                let signal = ExecutionSignal::pump_fun_buy(
                    target.config.clone(),
                    slot,
                    spend,
                    operator_tokens,
                    ExecutionPayload::PumpFunBuy(buy_payload),
                    self.parsing_time_us,
                    compute_units_consumed,
                );
                self.emit_execution_signal(signal);
                false
            }
            (false, ExecutionPayload::PumpFunSell(mut sell_payload)) => {
                if !target.config.mirror_sells {
                    return false;
                }
                let operator_available = self.operator.total_tokens(mint);
                let sell_tokens =
                    target
                        .mirror_positions
                        .plan_sell(*mint, *token_amount, operator_available);
                if sell_tokens == 0 {
                    return false;
                }
                let target_holdings_pre = balances
                    .by_owner_mint(user, mint)
                    .and_then(|change| change.pre_amount)
                    .unwrap_or(0);
                let target_pct = if target_holdings_pre > 0 {
                    (*token_amount as f64 / target_holdings_pre as f64) * 100.0
                } else {
                    0.0
                };
                let operator_pct = if operator_available > 0 {
                    (sell_tokens as f64 / operator_available as f64) * 100.0
                } else {
                    0.0
                };
                info!(
                    "Target {} sold {:.4} tokens for {:.4} SOL (sold {:.2}% of position); mirroring {:.2}% of operator holdings (mint {})",
                    user,
                    tokens_to_amount(*token_amount),
                    lamports_to_sol(*sol_amount),
                    target_pct,
                    operator_pct,
                    mint
                );
                sell_payload.amount = sell_tokens;
                let expected_sol = get_sell_sol_amount_from_token_amount(
                    (*virtual_token_reserves).into(),
                    (*virtual_sol_reserves).into(),
                    sell_tokens,
                );
                let signal = ExecutionSignal::pump_fun_sell(
                    target.config.clone(),
                    slot,
                    expected_sol,
                    sell_tokens,
                    ExecutionPayload::PumpFunSell(sell_payload),
                    self.parsing_time_us,
                    compute_units_consumed,
                );
                self.emit_execution_signal(signal);
                false
            }
            (_, other) => {
                debug!("Unexpected PumpFun payload variant: {:?}", other);
                false
            }
        }
    }

    /// Update target wallet state and potentially emit trade signals from a PumpAmmOperation.
    fn update_pump_amm_targets_from_action(
        &mut self,
        op: &PumpAmmOperation,
        balances: &TokenBalanceIndex<'_>,
        compute_units_consumed: Option<u64>,
    ) -> bool {
        let event = match &op.event {
            Some(ev) => ev,
            None => return false,
        };
        
        match event {
            PumpAmmEvent::BuyEvent {
                user,
                user_base_token_account,
                user_quote_token_account,
                base_amount_out,
                user_quote_amount_in,
                pool,
                ..
            } => {
                let Some(target) = self.targets.get_mut(user) else {
                    debug!(
                        "PumpAmm buy from non-target wallet {} (pool {})",
                        user, pool
                    );
                    return false;
                };
                if *user_quote_amount_in < MIN_TARGET_SOL_LAMPORTS {
                    debug!(
                        "Skipping PumpAmm buy mirror for {} due to small spend {} lamports",
                        user, user_quote_amount_in
                    );
                    return false;
                }
                let slot = event_slot(event);
                Self::apply_base_token_update(
                    &mut target.pump_amm,
                    balances.by_token_account(user_base_token_account),
                    slot,
                    Some(*base_amount_out),
                    None,
                );
                Self::apply_quote_update(
                    &mut target.pump_amm,
                    balances.by_token_account(user_quote_token_account),
                    slot,
                    Some(*user_quote_amount_in),
                );
                // Build payload from merged operation
                let mut payload = match build_pump_amm_buy_payload_from_operation(op) {
                    Some(ExecutionPayload::PumpAmmBuy(payload)) => payload,
                    Some(other) => {
                        debug!("Unexpected Pump AMM buy payload variant: {:?}", other);
                        return false;
                    }
                    None => return true,
                };
                info!(
                    "Target {} bought {:.4} tokens for {:.4} SOL (pool {})",
                    user,
                    tokens_to_amount(*base_amount_out),
                    lamports_to_sol(*user_quote_amount_in),
                    payload.accounts.pool
                );
                target
                    .pump_amm_positions
                    .record_target_buy(payload.accounts.base_mint, *base_amount_out);
                if target.config.mirror_sells {
                    target
                        .mirror_positions
                        .record_target_buy(payload.accounts.base_mint, *base_amount_out);
                }
                let mut spend = self.config.buy_amount_lamports();
                if spend == 0 {
                    spend = *user_quote_amount_in;
                    debug!(
                        "BUY_AMOUNT_SOL not configured; mirroring target spend {} lamports",
                        spend
                    );
                }
                if spend == 0 {
                    return false;
                }
                let operator_tokens = match buy_quote_input_internal(
                    spend,
                    0,
                    payload.base_reserve,
                    payload.quote_reserve,
                    payload.fees,
                ) {
                    Ok(result) => result.base,
                    Err(err) => {
                        warn!("Pump AMM buy math failed: {err}");
                        return false;
                    }
                };
                if operator_tokens == 0 {
                    return false;
                }
                info!(
                    "Buying {:.4} tokens for {:.4} SOL (pool {}, target {})",
                    tokens_to_amount(operator_tokens),
                    lamports_to_sol(spend),
                    payload.accounts.pool,
                    user
                );
                payload.spendable_quote_in = spend;
                let signal = ExecutionSignal::pump_amm_buy(
                    target.config.clone(),
                    slot,
                    spend,
                    operator_tokens,
                    ExecutionPayload::PumpAmmBuy(payload),
                    self.parsing_time_us,
                    compute_units_consumed,
                );
                self.emit_execution_signal(signal);
                false
            }
            PumpAmmEvent::SellEvent {
                user,
                user_base_token_account,
                user_quote_token_account,
                base_amount_in,
                user_quote_amount_out,
                pool,
                ..
            } => {
                let Some(target) = self.targets.get_mut(user) else {
                    debug!(
                        "PumpAmm sell from non-target wallet {} (pool {})",
                        user, pool
                    );
                    return false;
                };
                let slot = event_slot(event);
                Self::apply_base_token_update(
                    &mut target.pump_amm,
                    balances.by_token_account(user_base_token_account),
                    slot,
                    Some(*base_amount_in),
                    None,
                );
                Self::apply_quote_update(
                    &mut target.pump_amm,
                    balances.by_token_account(user_quote_token_account),
                    slot,
                    Some(*user_quote_amount_out),
                );
                // Build payload from merged operation
                let mut payload = match build_pump_amm_sell_payload_from_operation(op) {
                    Some(ExecutionPayload::PumpAmmSell(payload)) => payload,
                    Some(other) => {
                        debug!("Unexpected Pump AMM sell payload variant: {:?}", other);
                        return false;
                    }
                    None => return true,
                };
                if !target.config.mirror_sells {
                    return false;
                }
                let base_mint = payload.accounts.base_mint;
                let operator_available = self.operator.total_tokens(&base_mint);
                let sell_tokens = target.mirror_positions.plan_sell(
                    base_mint,
                    *base_amount_in,
                    operator_available,
                );
                if sell_tokens == 0 {
                    return false;
                }
                let target_holdings_pre = balances
                    .by_token_account(user_base_token_account)
                    .and_then(|change| change.pre_amount)
                    .unwrap_or(0);
                let target_pct = if target_holdings_pre > 0 {
                    (*base_amount_in as f64 / target_holdings_pre as f64) * 100.0
                } else {
                    0.0
                };
                let operator_pct = if operator_available > 0 {
                    (sell_tokens as f64 / operator_available as f64) * 100.0
                } else {
                    0.0
                };
                info!(
                    "Target {} sold {:.4} tokens for {:.4} SOL (sold {:.2}% of position); mirroring {:.2}% of operator holdings (pool {})",
                    user,
                    tokens_to_amount(*base_amount_in),
                    lamports_to_sol(*user_quote_amount_out),
                    target_pct,
                    operator_pct,
                    payload.accounts.pool
                );
                payload.base_amount_in = sell_tokens;
                let slippage_bps = slippage_pct_to_bps(target.config.slippage_pct) as u64;
                let quote_result = match sell_base_input_internal(
                    sell_tokens,
                    slippage_bps,
                    payload.base_reserve,
                    payload.quote_reserve,
                    payload.fees,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        warn!("Pump AMM sell math failed: {err}");
                        return false;
                    }
                };
                payload.expected_sol_out = quote_result.ui_quote;
                let signal = ExecutionSignal::pump_amm_sell(
                    target.config.clone(),
                    slot,
                    quote_result.ui_quote,
                    sell_tokens,
                    ExecutionPayload::PumpAmmSell(payload),
                    self.parsing_time_us,
                    compute_units_consumed,
                );
                self.emit_execution_signal(signal);
                false
            }
        }
    }

    fn apply_base_token_update(
        holdings: &mut WalletHoldings,
        change: Option<&TokenBalanceChange>,
        slot: u64,
        fallback_amount: Option<u64>,
        fallback_mint: Option<Pubkey>,
    ) {
        if let Some(change) = change {
            if let Some(post_amount) = change.post_amount {
                holdings.update_token_position(
                    change.key.mint,
                    change.token_account,
                    post_amount,
                    change.decimals,
                    slot,
                );
                return;
            }
        }

        if let (Some(amount), Some(mint)) = (fallback_amount, fallback_mint) {
            holdings.update_token_position(mint, None, amount, DEFAULT_TOKEN_DECIMALS, slot);
        }
    }

    fn apply_operator_fill(
        &mut self,
        pending: &PendingConfirmation,
        parsed_tx: &ParsedTransaction,
    ) {
        let Some(target) = self.targets.get_mut(&pending.target) else {
            warn!(
                "Confirmed transaction for unknown target {}",
                pending.target
            );
            return;
        };
        let operator_key = self.operator.wallet;
        // Iterate over actions (merged instruction+event pairs)
        for action in &parsed_tx.actions {
            match (pending.protocol, action) {
                (TradeProtocol::PumpFun, ParsedAction::PumpFun(op)) => {
                    if let Some(PumpFunEvent::TradeEvent {
                        user,
                        mint,
                        token_amount,
                        is_buy: true,
                        ..
                    }) = &op.event
                    {
                        if user == &operator_key {
                            target
                                .pump_fun_positions
                                .record_operator_buy(*mint, *token_amount);
                            target
                                .mirror_positions
                                .record_operator_buy(*mint, *token_amount);
                        }
                    }
                }
                (TradeProtocol::PumpAmm, ParsedAction::PumpAmm(op)) => {
                    if let Some(PumpAmmEvent::BuyEvent {
                        user,
                        base_amount_out,
                        ..
                    }) = &op.event
                    {
                        if user == &operator_key {
                            target
                                .pump_amm_positions
                                .record_operator_buy(pending.mint, *base_amount_out);
                            target
                                .mirror_positions
                                .record_operator_buy(pending.mint, *base_amount_out);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn apply_quote_update(
        holdings: &mut WalletHoldings,
        change: Option<&TokenBalanceChange>,
        slot: u64,
        fallback_lamports: Option<u64>,
    ) {
        if let Some(change) = change {
            if let Some(post_amount) = change.post_amount {
                if change.key.mint == native_mint::id() {
                    holdings.update_sol_balance_from_token_amount(
                        post_amount,
                        change.decimals,
                        slot,
                    );
                } else {
                    holdings.update_token_position(
                        change.key.mint,
                        change.token_account,
                        post_amount,
                        change.decimals,
                        slot,
                    );
                }
                return;
            }
        }

        if let Some(lamports) = fallback_lamports {
            holdings.update_sol_balance_from_lamports(lamports, slot);
        }
    }
}

/// Build an execution payload from a PumpFunOperation (merged instruction + event).
/// The new parsing mechanism bundles the instruction with its event, eliminating the need
/// for InstructionIndex lookups.
fn build_pump_fun_payload_from_operation(
    op: &PumpFunOperation,
) -> Option<ExecutionPayload> {
    let event = op.event.as_ref()?;
    
    let PumpFunEvent::TradeEvent {
        is_buy,
        virtual_token_reserves,
        virtual_sol_reserves,
        real_token_reserves,
        ..
    } = event;

    match (&op.instruction, *is_buy) {
        (
            PumpFunInstruction::BuyExactSolIn {
                spendable_sol_in,
                track_volume,
                global,
                fee_recipient,
                mint,
                bonding_curve,
                associated_bonding_curve,
                associated_user,
                user,
                system_program,
                token_program,
                creator_vault,
                event_authority,
                program,
                global_volume_accumulator,
                user_volume_accumulator,
                fee_config,
                fee_program,
                ..
            },
            true,
        ) => {
            let accounts = BuyExactSolInAccounts {
                global: *global,
                fee_recipient: *fee_recipient,
                mint: *mint,
                bonding_curve: *bonding_curve,
                associated_bonding_curve: *associated_bonding_curve,
                associated_user: *associated_user,
                user: *user,
                system_program: *system_program,
                token_program: *token_program,
                creator_vault: *creator_vault,
                event_authority: *event_authority,
                program: *program,
                global_volume_accumulator: *global_volume_accumulator,
                user_volume_accumulator: *user_volume_accumulator,
                fee_config: *fee_config,
                fee_program: *fee_program,
            };
            Some(ExecutionPayload::PumpFunBuy(PumpFunBuyPayload {
                accounts,
                spendable_sol_in: *spendable_sol_in,
                virtual_token_reserves: (*virtual_token_reserves).into(),
                virtual_sol_reserves: (*virtual_sol_reserves).into(),
                real_token_reserves: (*real_token_reserves).into(),
                track_volume: track_volume.unwrap_or(false),
            }))
        }
        (
            PumpFunInstruction::Buy {
                max_sol_cost,
                track_volume,
                global,
                fee_recipient,
                mint,
                bonding_curve,
                associated_bonding_curve,
                associated_user,
                user,
                system_program,
                token_program,
                creator_vault,
                event_authority,
                program,
                global_volume_accumulator,
                user_volume_accumulator,
                fee_config,
                fee_program,
                ..
            },
            true,
        ) => {
            let accounts = BuyExactSolInAccounts {
                global: *global,
                fee_recipient: *fee_recipient,
                mint: *mint,
                bonding_curve: *bonding_curve,
                associated_bonding_curve: *associated_bonding_curve,
                associated_user: *associated_user,
                user: *user,
                system_program: *system_program,
                token_program: *token_program,
                creator_vault: *creator_vault,
                event_authority: *event_authority,
                program: *program,
                global_volume_accumulator: *global_volume_accumulator,
                user_volume_accumulator: *user_volume_accumulator,
                fee_config: *fee_config,
                fee_program: *fee_program,
            };
            Some(ExecutionPayload::PumpFunBuy(PumpFunBuyPayload {
                accounts,
                spendable_sol_in: *max_sol_cost,
                virtual_token_reserves: (*virtual_token_reserves).into(),
                virtual_sol_reserves: (*virtual_sol_reserves).into(),
                real_token_reserves: (*real_token_reserves).into(),
                track_volume: track_volume.unwrap_or(false),
            }))
        }
        (
            PumpFunInstruction::Sell {
                amount,
                global,
                fee_recipient,
                mint,
                bonding_curve,
                associated_bonding_curve,
                associated_user,
                user,
                system_program,
                creator_vault,
                token_program,
                event_authority,
                program,
                fee_config,
                fee_program,
                ..
            },
            false,
        ) => {
            let accounts = PumpFunSellAccounts {
                global: *global,
                fee_recipient: *fee_recipient,
                mint: *mint,
                bonding_curve: *bonding_curve,
                associated_bonding_curve: *associated_bonding_curve,
                associated_user: *associated_user,
                user: *user,
                system_program: *system_program,
                creator_vault: *creator_vault,
                token_program: *token_program,
                event_authority: *event_authority,
                program: *program,
                fee_config: *fee_config,
                fee_program: *fee_program,
            };
            Some(ExecutionPayload::PumpFunSell(PumpFunSellPayload {
                accounts,
                amount: *amount,
                virtual_token_reserves: (*virtual_token_reserves).into(),
                virtual_sol_reserves: (*virtual_sol_reserves).into(),
            }))
        }
        _ => None,
    }
}

/// Build a Pump AMM buy payload from a PumpAmmOperation (merged instruction + event).
fn build_pump_amm_buy_payload_from_operation(
    op: &PumpAmmOperation,
) -> Option<ExecutionPayload> {
    let event = op.event.as_ref()?;
    
    let PumpAmmEvent::BuyEvent {
        pool_base_token_reserves,
        pool_quote_token_reserves,
        lp_fee_basis_points,
        protocol_fee_basis_points,
        coin_creator_fee_basis_points,
        ..
    } = event
    else {
        return None;
    };

    // Extract spend amount and track_volume from the Buy instruction
    let (quote_in, track_volume, accounts) = match &op.instruction {
        PumpAmmInstruction::Buy {
            max_quote_amount_in,
            track_volume,
            pool,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base_token_account,
            user_quote_token_account,
            pool_base_token_account,
            pool_quote_token_account,
            protocol_fee_recipient,
            protocol_fee_recipient_token_account,
            base_token_program,
            quote_token_program,
            system_program,
            associated_token_program,
            event_authority,
            program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            global_volume_accumulator,
            user_volume_accumulator,
            fee_config,
            fee_program,
            ..
        } => (
            *max_quote_amount_in,
            track_volume.unwrap_or(false),
            BuyExactQuoteInAccounts {
                pool: *pool,
                user: *user,
                global_config: *global_config,
                base_mint: *base_mint,
                quote_mint: *quote_mint,
                user_base_token_account: *user_base_token_account,
                user_quote_token_account: *user_quote_token_account,
                pool_base_token_account: *pool_base_token_account,
                pool_quote_token_account: *pool_quote_token_account,
                protocol_fee_recipient: *protocol_fee_recipient,
                protocol_fee_recipient_token_account: *protocol_fee_recipient_token_account,
                base_token_program: *base_token_program,
                quote_token_program: *quote_token_program,
                system_program: *system_program,
                associated_token_program: *associated_token_program,
                event_authority: *event_authority,
                program: *program,
                coin_creator_vault_ata: *coin_creator_vault_ata,
                coin_creator_vault_authority: *coin_creator_vault_authority,
                global_volume_accumulator: *global_volume_accumulator,
                user_volume_accumulator: *user_volume_accumulator,
                fee_config: *fee_config,
                fee_program: *fee_program,
            },
        ),
        _ => return None,
    };

    let fees = Fees {
        lp_fee_bps: (*lp_fee_basis_points).min(u16::MAX as u64) as u16,
        protocol_fee_bps: (*protocol_fee_basis_points).min(u16::MAX as u64) as u16,
        coin_creator_fee_bps: (*coin_creator_fee_basis_points).min(u16::MAX as u64) as u16,
    };

    Some(ExecutionPayload::PumpAmmBuy(PumpAmmBuyPayload {
        accounts,
        spendable_quote_in: quote_in,
        base_reserve: *pool_base_token_reserves,
        quote_reserve: *pool_quote_token_reserves,
        fees,
        track_volume,
    }))
}

/// Build a Pump AMM sell payload from a PumpAmmOperation (merged instruction + event).
fn build_pump_amm_sell_payload_from_operation(
    op: &PumpAmmOperation,
) -> Option<ExecutionPayload> {
    let event = op.event.as_ref()?;
    
    let PumpAmmEvent::SellEvent {
        pool_base_token_reserves,
        pool_quote_token_reserves,
        lp_fee_basis_points,
        protocol_fee_basis_points,
        coin_creator_fee_basis_points,
        user_quote_amount_out,
        ..
    } = event
    else {
        return None;
    };

    if let PumpAmmInstruction::Sell {
        base_amount_in,
        pool,
        user,
        global_config,
        base_mint,
        quote_mint,
        user_base_token_account,
        user_quote_token_account,
        pool_base_token_account,
        pool_quote_token_account,
        protocol_fee_recipient,
        protocol_fee_recipient_token_account,
        base_token_program,
        quote_token_program,
        system_program,
        associated_token_program,
        event_authority,
        program,
        coin_creator_vault_ata,
        coin_creator_vault_authority,
        fee_config,
        fee_program,
        ..
    } = &op.instruction
    {
        let accounts = PumpAmmSellAccounts {
            pool: *pool,
            user: *user,
            global_config: *global_config,
            base_mint: *base_mint,
            quote_mint: *quote_mint,
            user_base_token_account: *user_base_token_account,
            user_quote_token_account: *user_quote_token_account,
            pool_base_token_account: *pool_base_token_account,
            pool_quote_token_account: *pool_quote_token_account,
            protocol_fee_recipient: *protocol_fee_recipient,
            protocol_fee_recipient_token_account: *protocol_fee_recipient_token_account,
            base_token_program: *base_token_program,
            quote_token_program: *quote_token_program,
            system_program: *system_program,
            associated_token_program: *associated_token_program,
            event_authority: *event_authority,
            program: *program,
            coin_creator_vault_ata: *coin_creator_vault_ata,
            coin_creator_vault_authority: *coin_creator_vault_authority,
            fee_config: *fee_config,
            fee_program: *fee_program,
        };
        let fees = Fees {
            lp_fee_bps: (*lp_fee_basis_points).min(u16::MAX as u64) as u16,
            protocol_fee_bps: (*protocol_fee_basis_points).min(u16::MAX as u64) as u16,
            coin_creator_fee_bps: (*coin_creator_fee_basis_points).min(u16::MAX as u64) as u16,
        };
        return Some(ExecutionPayload::PumpAmmSell(PumpAmmSellPayload {
            accounts,
            base_amount_in: *base_amount_in,
            base_reserve: *pool_base_token_reserves,
            quote_reserve: *pool_quote_token_reserves,
            fees,
            expected_sol_out: *user_quote_amount_out,
        }));
    }

    None
}
#[derive(Clone)]
struct WalletTargetState {
    config: TargetWalletConfig,
    pump_fun: WalletHoldings,
    pump_amm: WalletHoldings,
    pump_fun_positions: MirrorLedger,
    pump_amm_positions: MirrorLedger,
    mirror_positions: MirrorLedger,
    tp_sl_positions: HashMap<Pubkey, TpSlPosition>,
}

impl WalletTargetState {
    fn new(config: TargetWalletConfig) -> Self {
        Self {
            config,
            pump_fun: WalletHoldings::default(),
            pump_amm: WalletHoldings::default(),
            pump_fun_positions: MirrorLedger::default(),
            pump_amm_positions: MirrorLedger::default(),
            mirror_positions: MirrorLedger::default(),
            tp_sl_positions: HashMap::new(),
        }
    }

    fn snapshot(&self) -> WalletSnapshot {
        WalletSnapshot {
            wallet: self.config.wallet,
            slippage_pct: self.config.slippage_pct,
            pump_fun: HoldingSnapshot::from(&self.pump_fun),
            pump_amm: HoldingSnapshot::from(&self.pump_amm),
        }
    }
}

#[derive(Default)]
struct PositionIndex {
    index: HashMap<Pubkey, HashSet<Pubkey>>,
}

impl PositionIndex {
    fn insert(&mut self, mint: Pubkey, wallet: Pubkey) {
        self.index.entry(mint).or_default().insert(wallet);
    }

    fn remove(&mut self, mint: &Pubkey, wallet: &Pubkey) {
        if let Some(targets) = self.index.get_mut(mint) {
            targets.remove(wallet);
            if targets.is_empty() {
                self.index.remove(mint);
            }
        }
    }

    fn get_targets(&self, mint: &Pubkey) -> Option<&HashSet<Pubkey>> {
        self.index.get(mint)
    }

    fn targets_for(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.index
            .get(mint)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct TpSlPosition {
    protocol: TradeProtocol,
    buy_price: f64,
    take_profit_pct: Option<f64>,
    stop_loss_pct: Option<f64>,
    pending_sell: bool,
    last_log_at: Instant,
    drift_baseline_price: Option<f64>,
    drift_baseline_at: Option<Instant>,
}

#[derive(Clone, Copy)]
enum TpSlTrigger {
    TakeProfit,
    StopLoss,
    Stagnation,
}

struct TpSlAction {
    signal: ExecutionSignal,
    target_wallet: Pubkey,
    mint: Pubkey,
    trigger: TpSlTrigger,
    current_price: f64,
    pct_change: f64,
}

impl TpSlPosition {
    fn new(
        protocol: TradeProtocol,
        buy_price: f64,
        take_profit_pct: Option<f64>,
        stop_loss_pct: Option<f64>,
    ) -> Self {
        Self {
            protocol,
            buy_price,
            take_profit_pct,
            stop_loss_pct,
            pending_sell: false,
            last_log_at: Instant::now(),
            drift_baseline_price: None,
            drift_baseline_at: None,
        }
    }

    fn tp_price(&self) -> Option<f64> {
        self.take_profit_pct
            .map(|pct| self.buy_price * (1.0 + pct / 100.0))
    }

    fn sl_price(&self) -> Option<f64> {
        self.stop_loss_pct
            .map(|pct| self.buy_price * (1.0 - pct / 100.0))
    }

    fn percentage_change(&self, current_price: f64) -> f64 {
        if self.buy_price <= f64::EPSILON {
            return 0.0;
        }
        ((current_price - self.buy_price) / self.buy_price) * 100.0
    }

    fn evaluate_trigger(&self, pct_change: f64) -> Option<TpSlTrigger> {
        if let Some(tp) = self.take_profit_pct {
            if pct_change >= tp {
                return Some(TpSlTrigger::TakeProfit);
            }
        }
        if let Some(sl) = self.stop_loss_pct {
            if pct_change <= -sl {
                return Some(TpSlTrigger::StopLoss);
            }
        }
        None
    }

    fn record_additional_buy(&mut self, previous_tokens: u64, new_tokens: u64, new_price: f64) {
        if new_tokens == 0 {
            return;
        }
        if previous_tokens == 0 || self.buy_price <= 0.0 {
            self.buy_price = new_price;
            return;
        }
        let total = previous_tokens.saturating_add(new_tokens);
        if total == 0 {
            self.buy_price = new_price;
            return;
        }
        let blended = ((self.buy_price * previous_tokens as f64) + (new_price * new_tokens as f64))
            / total as f64;
        self.buy_price = blended;
    }

    fn should_log(&self) -> bool {
        self.last_log_at.elapsed() >= Duration::from_secs(5)
    }

    fn mark_logged(&mut self) {
        self.last_log_at = Instant::now();
    }

    fn should_force_exit(
        &mut self,
        current_price: f64,
        threshold_pct: f64,
        window: Duration,
    ) -> bool {
        let now = Instant::now();
        match (self.drift_baseline_price, self.drift_baseline_at) {
            (Some(base), Some(last)) if now.duration_since(last) >= window => {
                self.drift_baseline_price = Some(current_price);
                self.drift_baseline_at = Some(now);
                if base.abs() <= f64::EPSILON {
                    return true;
                }
                let pct_change = ((current_price - base) / base) * 100.0;
                pct_change.abs() <= threshold_pct
            }
            (None, _) | (_, None) => {
                self.drift_baseline_price = Some(current_price);
                self.drift_baseline_at = Some(now);
                false
            }
            _ => false,
        }
    }
}

struct OperatorState {
    wallet: Pubkey,
    pump_fun: WalletHoldings,
    pump_amm: WalletHoldings,
}

impl OperatorState {
    fn new(wallet: Pubkey) -> Self {
        Self {
            wallet,
            pump_fun: WalletHoldings::default(),
            pump_amm: WalletHoldings::default(),
        }
    }

    fn pump_fun_tokens(&self, mint: &Pubkey) -> u64 {
        self.pump_fun.token_amount(mint)
    }

    fn pump_amm_tokens(&self, mint: &Pubkey) -> u64 {
        self.pump_amm.token_amount(mint)
    }

    fn total_tokens(&self, mint: &Pubkey) -> u64 {
        self.pump_fun_tokens(mint)
            .saturating_add(self.pump_amm_tokens(mint))
    }
}

#[derive(Clone, Default)]
struct WalletHoldings {
    sol_amount: f64,
    last_update_slot: Option<u64>,
    tokens: HashMap<Pubkey, TokenPosition>,
}

#[derive(Clone, Copy, Debug, Default)]
struct TokenPosition {
    token_account: Option<Pubkey>,
    raw_amount: u64,
    amount: f64,
    decimals: u8,
    last_update_slot: Option<u64>,
}

impl WalletHoldings {
    fn update_token_position(
        &mut self,
        mint: Pubkey,
        token_account: Option<Pubkey>,
        raw_amount: u64,
        decimals: u8,
        slot: u64,
    ) {
        let entry = self.tokens.entry(mint).or_default();
        entry.token_account = token_account;
        entry.raw_amount = raw_amount;
        entry.amount = amount_to_ui(raw_amount, decimals);
        entry.decimals = decimals;
        entry.last_update_slot = Some(slot);
        self.last_update_slot = Some(slot);
    }

    fn update_sol_balance_from_token_amount(&mut self, raw_amount: u64, decimals: u8, slot: u64) {
        self.sol_amount = amount_to_ui(raw_amount, decimals);
        self.last_update_slot = Some(slot);
    }

    fn update_sol_balance_from_lamports(&mut self, lamports: u64, slot: u64) {
        self.sol_amount = lamports_to_sol(lamports);
        self.last_update_slot = Some(slot);
    }

    fn token_amount(&self, mint: &Pubkey) -> u64 {
        self.tokens.get(mint).map(|pos| pos.raw_amount).unwrap_or(0)
    }
}

#[derive(Clone, Default)]
struct MirrorLedger {
    positions: HashMap<Pubkey, MirrorPosition>,
}

#[derive(Clone, Default)]
struct MirrorPosition {
    target_buys: u128,
    target_sells: u128,
    operator_buys: u128,
    operator_sells: u128,
}

impl MirrorLedger {
    fn record_target_buy(&mut self, mint: Pubkey, target_tokens: u64) {
        if target_tokens == 0 {
            return;
        }
        let entry = self.positions.entry(mint).or_default();
        entry.target_buys = entry.target_buys.saturating_add(target_tokens as u128);
    }

    fn record_operator_buy(&mut self, mint: Pubkey, operator_tokens: u64) {
        if operator_tokens == 0 {
            return;
        }
        let entry = self.positions.entry(mint).or_default();
        entry.operator_buys = entry.operator_buys.saturating_add(operator_tokens as u128);
        if entry.operator_sells > entry.operator_buys {
            entry.operator_sells = entry.operator_buys;
        }
        self.maybe_close(mint);
    }

    fn plan_sell(&mut self, mint: Pubkey, target_sell_tokens: u64, operator_available: u64) -> u64 {
        if target_sell_tokens == 0 || operator_available == 0 {
            return 0;
        }
        let Some(entry) = self.positions.get_mut(&mint) else {
            return 0;
        };
        if entry.target_buys == 0 || entry.operator_buys == 0 {
            return 0;
        }

        entry.target_sells = entry
            .target_sells
            .saturating_add(target_sell_tokens as u128)
            .min(entry.target_buys);

        let desired_operator_sold =
            entry.operator_buys.saturating_mul(entry.target_sells) / entry.target_buys;
        let pending = desired_operator_sold.saturating_sub(entry.operator_sells);
        if pending == 0 {
            return 0;
        }

        let mirrored_available = entry
            .operator_buys
            .saturating_sub(entry.operator_sells)
            .min(operator_available as u128);

        if mirrored_available == 0 {
            return 0;
        }

        let sell_amount = pending.min(mirrored_available) as u64;
        if sell_amount == 0 {
            return 0;
        }

        entry.operator_sells = entry
            .operator_sells
            .saturating_add(sell_amount as u128)
            .min(entry.operator_buys);

        self.maybe_close(mint);

        sell_amount
    }

    fn maybe_close(&mut self, mint: Pubkey) {
        if let Some(entry) = self.positions.get(&mint) {
            if entry.operator_sells >= entry.operator_buys
                && entry.target_sells >= entry.target_buys
            {
                self.positions.remove(&mint);
            }
        }
    }
}

struct PumpFunTracker {
    holdings: HashMap<Pubkey, PumpFunWalletState>,
}

impl PumpFunTracker {
    fn new() -> Self {
        Self {
            holdings: HashMap::new(),
        }
    }

    fn handle_instruction(&mut self, _instruction: &crate::parsers::pump_fun::PumpFunInstruction) {}

    fn handle_event(&mut self, event: &PumpFunEvent) {
        match event {
            PumpFunEvent::TradeEvent { user, .. } => {
                debug!("Received PumpFun trade event for user {}", user);
                let entry = self
                    .holdings
                    .entry(*user)
                    .or_insert_with(PumpFunWalletState::default);
                entry.update_from_event(event);
            }
        }
    }
}

#[derive(Default)]
struct PumpFunWalletState {
    sol_amount: f64,
    token_amount: f64,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    real_sol_reserves: u64,
    real_token_reserves: u64,
    last_update_slot: Option<u64>,
}

impl PumpFunWalletState {
    fn update_from_event(&mut self, event: &PumpFunEvent) {
        match event {
            PumpFunEvent::TradeEvent {
                sol_amount,
                token_amount,
                virtual_sol_reserves,
                virtual_token_reserves,
                real_sol_reserves,
                real_token_reserves,
                ..
            } => {
                self.sol_amount = lamports_to_sol(*sol_amount);
                self.token_amount = tokens_to_amount(*token_amount);
                self.virtual_sol_reserves = *virtual_sol_reserves;
                self.virtual_token_reserves = *virtual_token_reserves;
                self.real_sol_reserves = *real_sol_reserves;
                self.real_token_reserves = *real_token_reserves;
                self.last_update_slot = Some(event_slot(event));
            }
        }
    }

    fn snapshot(&self) -> PumpFunSnapshot {
        PumpFunSnapshot {
            sol_amount: self.sol_amount,
            token_amount: self.token_amount,
            virtual_sol_reserves: self.virtual_sol_reserves,
            virtual_token_reserves: self.virtual_token_reserves,
            real_sol_reserves: self.real_sol_reserves,
            real_token_reserves: self.real_token_reserves,
            last_update_slot: self.last_update_slot,
        }
    }
}

struct PumpAmmTracker {
    holdings: HashMap<Pubkey, PumpAmmWalletState>,
}

impl PumpAmmTracker {
    fn new() -> Self {
        Self {
            holdings: HashMap::new(),
        }
    }

    fn handle_instruction(&mut self, _instruction: &crate::parsers::pump_amm::PumpAmmInstruction) {}

    fn handle_event(&mut self, event: &PumpAmmEvent) {
        match event {
            PumpAmmEvent::BuyEvent {
                user,
                user_quote_amount_in,
                base_amount_out,
                pool_base_token_reserves,
                pool_quote_token_reserves,
                lp_fee_basis_points,
                protocol_fee_basis_points,
                coin_creator_fee_basis_points,
                ..
            } => {
                debug!("Received Pump AMM buy event for user {}", user);
                let entry = self
                    .holdings
                    .entry(*user)
                    .or_insert_with(PumpAmmWalletState::default);
                entry.sol_amount = lamports_to_sol(*user_quote_amount_in);
                entry.token_amount = tokens_to_amount(*base_amount_out);
                entry.pool_base_reserve = *pool_base_token_reserves;
                entry.pool_quote_reserve = *pool_quote_token_reserves;
                entry.lp_fee_bps = *lp_fee_basis_points;
                entry.protocol_fee_bps = *protocol_fee_basis_points;
                entry.coin_creator_fee_bps = *coin_creator_fee_basis_points;
                entry.last_update_slot = Some(event_slot(event));
            }
            PumpAmmEvent::SellEvent {
                user,
                user_quote_amount_out,
                base_amount_in,
                pool_base_token_reserves,
                pool_quote_token_reserves,
                lp_fee_basis_points,
                protocol_fee_basis_points,
                coin_creator_fee_basis_points,
                ..
            } => {
                debug!("Received Pump AMM sell event for user {}", user);
                let entry = self
                    .holdings
                    .entry(*user)
                    .or_insert_with(PumpAmmWalletState::default);
                entry.sol_amount = lamports_to_sol(*user_quote_amount_out);
                entry.token_amount = tokens_to_amount(*base_amount_in);
                entry.pool_base_reserve = *pool_base_token_reserves;
                entry.pool_quote_reserve = *pool_quote_token_reserves;
                entry.lp_fee_bps = *lp_fee_basis_points;
                entry.protocol_fee_bps = *protocol_fee_basis_points;
                entry.coin_creator_fee_bps = *coin_creator_fee_basis_points;
                entry.last_update_slot = Some(event_slot(event));
            }
        }
    }
}

#[derive(Default)]
struct PumpAmmWalletState {
    sol_amount: f64,
    token_amount: f64,
    pool_base_reserve: u64,
    pool_quote_reserve: u64,
    lp_fee_bps: u64,
    protocol_fee_bps: u64,
    coin_creator_fee_bps: u64,
    last_update_slot: Option<u64>,
}

impl PumpAmmWalletState {
    fn snapshot(&self) -> PumpAmmSnapshot {
        PumpAmmSnapshot {
            sol_amount: self.sol_amount,
            token_amount: self.token_amount,
            pool_base_reserve: self.pool_base_reserve,
            pool_quote_reserve: self.pool_quote_reserve,
            lp_fee_bps: self.lp_fee_bps,
            protocol_fee_bps: self.protocol_fee_bps,
            coin_creator_fee_bps: self.coin_creator_fee_bps,
            last_update_slot: self.last_update_slot,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalletSnapshot {
    pub wallet: Pubkey,
    pub slippage_pct: f64,
    pub pump_fun: HoldingSnapshot,
    pub pump_amm: HoldingSnapshot,
}

#[derive(Clone, Debug, Default)]
pub struct HoldingSnapshot {
    pub sol_amount: f64,
    pub last_update_slot: Option<u64>,
    pub tokens: HashMap<Pubkey, MintHoldingSnapshot>,
}

impl From<&WalletHoldings> for HoldingSnapshot {
    fn from(value: &WalletHoldings) -> Self {
        let tokens = value
            .tokens
            .iter()
            .map(|(mint, position)| (*mint, MintHoldingSnapshot::from(position)))
            .collect();
        HoldingSnapshot {
            sol_amount: value.sol_amount,
            last_update_slot: value.last_update_slot,
            tokens,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MintHoldingSnapshot {
    pub token_account: Option<Pubkey>,
    pub raw_amount: u64,
    pub amount: f64,
    pub decimals: u8,
    pub last_update_slot: Option<u64>,
}

impl From<&TokenPosition> for MintHoldingSnapshot {
    fn from(position: &TokenPosition) -> Self {
        MintHoldingSnapshot {
            token_account: position.token_account,
            raw_amount: position.raw_amount,
            amount: position.amount,
            decimals: position.decimals,
            last_update_slot: position.last_update_slot,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PumpFunSnapshot {
    pub sol_amount: f64,
    pub token_amount: f64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub last_update_slot: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PumpAmmSnapshot {
    pub sol_amount: f64,
    pub token_amount: f64,
    pub pool_base_reserve: u64,
    pub pool_quote_reserve: u64,
    pub lp_fee_bps: u64,
    pub protocol_fee_bps: u64,
    pub coin_creator_fee_bps: u64,
    pub last_update_slot: Option<u64>,
}

fn event_slot(event: &impl HasSlot) -> u64 {
    event.slot()
}

trait HasSlot {
    fn slot(&self) -> u64;
}

impl HasSlot for PumpFunEvent {
    fn slot(&self) -> u64 {
        match self {
            PumpFunEvent::TradeEvent { timestamp, .. } => normalize_timestamp(*timestamp),
        }
    }
}

impl HasSlot for PumpAmmEvent {
    fn slot(&self) -> u64 {
        match self {
            PumpAmmEvent::BuyEvent { timestamp, .. } => normalize_timestamp(*timestamp),
            PumpAmmEvent::SellEvent { timestamp, .. } => normalize_timestamp(*timestamp),
        }
    }
}

fn normalize_timestamp(value: i64) -> u64 {
    if value < 0 {
        0
    } else {
        value as u64
    }
}

fn slippage_pct_to_bps(pct: f64) -> u16 {
    let bps = (pct * 100.0).round();
    bps.clamp(0.0, 10_000.0) as u16
}

fn lamports_to_sol(value: u64) -> f64 {
    value as f64 / 1_000_000_000.0
}

fn tokens_to_amount(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn amount_to_ui(value: u64, decimals: u8) -> f64 {
    if decimals == 0 {
        return value as f64;
    }
    let scale = 10f64.powi(decimals as i32);
    value as f64 / scale
}

fn retarget_pump_fun_buy_accounts(accounts: &mut BuyExactSolInAccounts, operator: &Pubkey) {
    accounts.user = *operator;
    accounts.associated_user = derive_ata(operator, &accounts.mint, &accounts.token_program);
    accounts.user_volume_accumulator = pump_fun_volume_pda(operator);
}

fn retarget_pump_fun_sell_accounts(accounts: &mut PumpFunSellAccounts, operator: &Pubkey) {
    accounts.user = *operator;
    accounts.associated_user = derive_ata(operator, &accounts.mint, &accounts.token_program);
}

fn retarget_pump_amm_buy_accounts(accounts: &mut BuyExactQuoteInAccounts, operator: &Pubkey) {
    accounts.user = *operator;
    accounts.user_base_token_account =
        derive_ata(operator, &accounts.base_mint, &accounts.base_token_program);
    accounts.user_quote_token_account = derive_ata(
        operator,
        &accounts.quote_mint,
        &accounts.quote_token_program,
    );
    accounts.user_volume_accumulator = pump_amm_volume_pda(operator);
}

fn retarget_pump_amm_sell_accounts(accounts: &mut PumpAmmSellAccounts, operator: &Pubkey) {
    accounts.user = *operator;
    accounts.user_base_token_account =
        derive_ata(operator, &accounts.base_mint, &accounts.base_token_program);
    accounts.user_quote_token_account = derive_ata(
        operator,
        &accounts.quote_mint,
        &accounts.quote_token_program,
    );
}

fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(owner, mint, token_program)
}

fn format_price(value: Option<f64>) -> String {
    value
        .map(|price| format!("{:.6}", price))
        .unwrap_or_else(|| "N/A".to_string())
}

fn log_tp_sl_targets(
    target_wallet: Pubkey,
    mint: Pubkey,
    buy_price: f64,
    tp_price: Option<f64>,
    sl_price: Option<f64>,
) {
    info!(
        "TP/SL | Target {} mint {} | Buy Price: {:.6} SOL | Target Price (TP): {} | Target Price (SL): {}",
        target_wallet,
        mint,
        buy_price,
        format_price(tp_price),
        format_price(sl_price),
    );
}

fn log_tp_sl_status(
    target_wallet: Pubkey,
    mint: Pubkey,
    position: &TpSlPosition,
    current_price: f64,
    pct_change: f64,
) {
    info!(
        "TP/SL | Target {} mint {} | Buy Price: {:.6} SOL | Current Price: {:.6} SOL | Target Price (TP): {} | Target Price (SL): {} | Current Percentage: {:+.2}%",
        target_wallet,
        mint,
        position.buy_price,
        current_price,
        format_price(position.tp_price()),
        format_price(position.sl_price()),
        pct_change,
    );
}

fn log_tp_sl_trigger(
    target_wallet: Pubkey,
    mint: Pubkey,
    trigger: TpSlTrigger,
    current_price: f64,
    pct_change: f64,
) {
    let reason = match trigger {
        TpSlTrigger::TakeProfit => "Take Profit",
        TpSlTrigger::StopLoss => "Stop Loss",
        TpSlTrigger::Stagnation => "Dead Trade",
    };
    info!(
        "TP/SL | Target {} mint {} | Triggered {} at {:.6} SOL ({:+.2}% vs buy price)",
        target_wallet, mint, reason, current_price, pct_change
    );
}

#[derive(Clone)]
pub struct GrpcDump {
    dir: PathBuf,
}

impl GrpcDump {
    pub fn new_from_env() -> anyhow::Result<Self> {
        let dir = std::env::var("GRPC_DUMP_DIR").unwrap_or_else(|_| "grpc_dumps".to_string());
        let path = PathBuf::from(dir);
        fs::create_dir_all(&path)?;
        Ok(Self { dir: path })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn dump_failed_transaction(
        &self,
        update: &SubscribeUpdateTransaction,
        parsed_tx: &ParsedTransaction,
    ) -> anyhow::Result<()> {
        let signature = update
            .transaction
            .as_ref()
            .map(|info| info.signature.clone())
            .filter(|sig| !sig.is_empty())
            .map(|sig| bs58::encode(sig).into_string())
            .unwrap_or_else(|| format!("slot-{}", update.slot));

        let mut path = self.dir.clone();
        path.push(format!("{signature}.json"));

        if path.exists() {
            return Ok(());
        }

        let payload = json!({
            "signature": signature,
            "slot": update.slot,
            "reason": "target_event_mismatch",
            "transaction": update
                .transaction
                .as_ref()
                .map(|info| serialize_transaction_info(info)),
            "parsed_summary": summarize_parsed_transaction(parsed_tx),
        });

        fs::write(&path, serde_json::to_string_pretty(&payload)?)?;
        Ok(())
    }
}

fn serialize_transaction_info(info: &SubscribeUpdateTransactionInfo) -> Value {
    json!({
        "signature": encode_signature(&info.signature),
        "is_vote": info.is_vote,
        "index": info.index,
        "transaction": info.transaction.as_ref().map(|tx| serialize_confirmed_transaction(tx)),
        "meta": info.meta.as_ref().map(|meta| serialize_meta(meta)),
    })
}

fn serialize_confirmed_transaction(tx: &confirmed_block::Transaction) -> Value {
    json!({
        "signatures": tx.signatures.iter().map(|sig| encode_signature(sig)).collect::<Vec<_>>(),
        "message": tx.message.as_ref().map(|msg| serialize_message(msg)),
    })
}

fn serialize_message(message: &confirmed_block::Message) -> Value {
    json!({
        "header": message.header.as_ref().map(|header| json!({
            "num_required_signatures": header.num_required_signatures,
            "num_readonly_signed_accounts": header.num_readonly_signed_accounts,
            "num_readonly_unsigned_accounts": header.num_readonly_unsigned_accounts,
        })),
        "account_keys": message
            .account_keys
            .iter()
            .map(|key| encode_pubkey_like(key))
            .collect::<Vec<_>>(),
        "recent_blockhash": encode_pubkey_like(&message.recent_blockhash),
        "instructions": message
            .instructions
            .iter()
            .map(|ix| serialize_compiled_instruction(ix))
            .collect::<Vec<_>>(),
        "versioned": message.versioned,
        "address_table_lookups": message
            .address_table_lookups
            .iter()
            .map(|lookup| serialize_lookup(lookup))
            .collect::<Vec<_>>(),
    })
}

fn serialize_compiled_instruction(ix: &confirmed_block::CompiledInstruction) -> Value {
    json!({
        "program_id_index": ix.program_id_index,
        "accounts": ix.accounts.clone(),
        "data": encode_data(&ix.data),
    })
}

fn serialize_lookup(lookup: &confirmed_block::MessageAddressTableLookup) -> Value {
    json!({
        "account_key": encode_pubkey_like(&lookup.account_key),
        "writable_indexes": lookup.writable_indexes.clone(),
        "readonly_indexes": lookup.readonly_indexes.clone(),
    })
}

fn serialize_meta(meta: &confirmed_block::TransactionStatusMeta) -> Value {
    json!({
        "err": meta.err.as_ref().map(|err| encode_data(&err.err)),
        "fee": meta.fee,
        "pre_balances": meta.pre_balances,
        "post_balances": meta.post_balances,
        "inner_instructions": meta
            .inner_instructions
            .iter()
            .map(|inner| serialize_inner_instructions(inner))
            .collect::<Vec<_>>(),
        "log_messages": meta.log_messages,
        "pre_token_balances": meta
            .pre_token_balances
            .iter()
            .map(|bal| serialize_token_balance(bal))
            .collect::<Vec<_>>(),
        "post_token_balances": meta
            .post_token_balances
            .iter()
            .map(|bal| serialize_token_balance(bal))
            .collect::<Vec<_>>(),
        "rewards": meta.rewards.iter().map(|reward| serialize_reward(reward)).collect::<Vec<_>>(),
        "loaded_writable_addresses": meta
            .loaded_writable_addresses
            .iter()
            .map(|addr| encode_pubkey_like(addr))
            .collect::<Vec<_>>(),
        "loaded_readonly_addresses": meta
            .loaded_readonly_addresses
            .iter()
            .map(|addr| encode_pubkey_like(addr))
            .collect::<Vec<_>>(),
        "return_data": meta.return_data.as_ref().map(|data| serialize_return_data(data)),
        "compute_units_consumed": meta.compute_units_consumed,
    })
}

fn serialize_inner_instructions(inner: &confirmed_block::InnerInstructions) -> Value {
    json!({
        "index": inner.index,
        "instructions": inner
            .instructions
            .iter()
            .map(|ix| serialize_inner_instruction(ix))
            .collect::<Vec<_>>(),
    })
}

fn serialize_inner_instruction(ix: &confirmed_block::InnerInstruction) -> Value {
    json!({
        "program_id_index": ix.program_id_index,
        "accounts": ix.accounts.clone(),
        "data": encode_data(&ix.data),
        "stack_height": ix.stack_height,
    })
}

fn serialize_token_balance(balance: &confirmed_block::TokenBalance) -> Value {
    json!({
        "account_index": balance.account_index,
        "mint": &balance.mint,
        "owner": &balance.owner,
        "program_id": &balance.program_id,
        "ui_token_amount": balance
            .ui_token_amount
            .as_ref()
            .map(|amount| serialize_ui_token_amount(amount)),
    })
}

fn serialize_ui_token_amount(amount: &confirmed_block::UiTokenAmount) -> Value {
    json!({
        "ui_amount": amount.ui_amount,
        "decimals": amount.decimals,
        "amount": amount.amount,
        "ui_amount_string": amount.ui_amount_string,
    })
}

fn serialize_reward(reward: &confirmed_block::Reward) -> Value {
    json!({
        "pubkey": reward.pubkey,
        "lamports": reward.lamports,
        "post_balance": reward.post_balance,
        "reward_type": reward.reward_type,
        "commission": reward.commission,
    })
}

fn serialize_return_data(data: &confirmed_block::ReturnData) -> Value {
    json!({
        "program_id": encode_pubkey_like(&data.program_id),
        "data": encode_data(&data.data),
    })
}

fn summarize_parsed_transaction(parsed_tx: &ParsedTransaction) -> Value {
    // Summarize actions (merged instruction+event pairs)
    let actions: Vec<_> = parsed_tx
        .actions
        .iter()
        .enumerate()
        .map(|(idx, action)| {
            match action {
                ParsedAction::PumpFun(op) => json!({
                    "index": idx,
                    "protocol": "PumpFun",
                    "instruction": format!("{:?}", std::mem::discriminant(&op.instruction)),
                    "has_event": op.event.is_some(),
                }),
                ParsedAction::PumpAmm(op) => json!({
                    "index": idx,
                    "protocol": "PumpAmm",
                    "instruction": format!("{:?}", std::mem::discriminant(&op.instruction)),
                    "has_event": op.event.is_some(),
                }),
            }
        })
        .collect();

    json!({
        "action_count": parsed_tx.actions.len(),
        "actions": actions,
        "token_balance_changes": parsed_tx.token_balances.len(),
    })
}

fn encode_signature(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    bs58::encode(bytes).into_string()
}

fn encode_pubkey_like(bytes: &[u8]) -> String {
    if bytes.len() == 32 {
        bs58::encode(bytes).into_string()
    } else {
        encode_data(bytes)
    }
}

fn encode_data(bytes: &[u8]) -> String {
    BASE64_ENGINE.encode(bytes)
}

struct TokenBalanceIndex<'a> {
    by_owner_mint: HashMap<TokenBalanceKey, &'a TokenBalanceChange>,
    by_account: HashMap<Pubkey, &'a TokenBalanceChange>,
}

impl<'a> TokenBalanceIndex<'a> {
    fn empty() -> Self {
        Self {
            by_owner_mint: HashMap::new(),
            by_account: HashMap::new(),
        }
    }

    fn new(balances: &'a [TokenBalanceChange]) -> Self {
        let mut by_owner_mint = HashMap::with_capacity(balances.len());
        let mut by_account = HashMap::with_capacity(balances.len());
        for balance in balances {
            by_owner_mint.insert(balance.key, balance);
            if let Some(token_account) = balance.token_account {
                by_account.insert(token_account, balance);
            }
        }
        Self {
            by_owner_mint,
            by_account,
        }
    }

    fn by_owner_mint(&self, owner: &Pubkey, mint: &Pubkey) -> Option<&'a TokenBalanceChange> {
        self.by_owner_mint
            .get(&TokenBalanceKey {
                owner: *owner,
                mint: *mint,
            })
            .copied()
    }

    fn by_token_account(&self, account: &Pubkey) -> Option<&'a TokenBalanceChange> {
        self.by_account.get(account).copied()
    }

    fn native_balance_for_owner(&self, owner: &Pubkey) -> Option<&'a TokenBalanceChange> {
        self.by_owner_mint(owner, &native_mint::id())
    }
}

// Note: InstructionIndex has been removed. The new parsing mechanism uses the "merge pattern"
// where instructions are bundled with their events at parse time (PumpFunOperation, PumpAmmOperation).

/// Extract sell accounts directly from a PumpFunOperation's instruction.
fn pump_fun_sell_accounts_from_operation(op: &PumpFunOperation) -> Option<PumpFunSellAccounts> {
    pump_fun_accounts_from_instruction(&op.instruction)
}

#[allow(unreachable_patterns)]
fn pump_fun_accounts_from_instruction(
    instruction: &PumpFunInstruction,
) -> Option<PumpFunSellAccounts> {
    match instruction {
        PumpFunInstruction::Buy {
            global,
            fee_recipient,
            mint,
            bonding_curve,
            associated_bonding_curve,
            associated_user,
            user,
            system_program,
            token_program,
            creator_vault,
            event_authority,
            program,
            fee_config,
            fee_program,
            ..
        }
        | PumpFunInstruction::BuyExactSolIn {
            global,
            fee_recipient,
            mint,
            bonding_curve,
            associated_bonding_curve,
            associated_user,
            user,
            system_program,
            token_program,
            creator_vault,
            event_authority,
            program,
            fee_config,
            fee_program,
            ..
        } => Some(PumpFunSellAccounts {
            global: *global,
            fee_recipient: *fee_recipient,
            mint: *mint,
            bonding_curve: *bonding_curve,
            associated_bonding_curve: *associated_bonding_curve,
            associated_user: *associated_user,
            user: *user,
            system_program: *system_program,
            creator_vault: *creator_vault,
            token_program: *token_program,
            event_authority: *event_authority,
            program: *program,
            fee_config: *fee_config,
            fee_program: *fee_program,
        }),
        PumpFunInstruction::Sell {
            global,
            fee_recipient,
            mint,
            bonding_curve,
            associated_bonding_curve,
            associated_user,
            user,
            system_program,
            creator_vault,
            token_program,
            event_authority,
            program,
            fee_config,
            fee_program,
            ..
        } => Some(PumpFunSellAccounts {
            global: *global,
            fee_recipient: *fee_recipient,
            mint: *mint,
            bonding_curve: *bonding_curve,
            associated_bonding_curve: *associated_bonding_curve,
            associated_user: *associated_user,
            user: *user,
            system_program: *system_program,
            creator_vault: *creator_vault,
            token_program: *token_program,
            event_authority: *event_authority,
            program: *program,
            fee_config: *fee_config,
            fee_program: *fee_program,
        }),
        _ => None,
    }
}

/// Extract sell accounts directly from a PumpAmmOperation's instruction.
fn pump_amm_sell_accounts_from_operation(op: &PumpAmmOperation) -> Option<(PumpAmmSellAccounts, Pubkey)> {
    pump_amm_accounts_from_instruction(&op.instruction)
}

#[allow(unreachable_patterns)]
fn pump_amm_accounts_from_instruction(
    instruction: &PumpAmmInstruction,
) -> Option<(PumpAmmSellAccounts, Pubkey)> {
    match instruction {
        PumpAmmInstruction::Buy {
            pool,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base_token_account,
            user_quote_token_account,
            pool_base_token_account,
            pool_quote_token_account,
            protocol_fee_recipient,
            protocol_fee_recipient_token_account,
            base_token_program,
            quote_token_program,
            system_program,
            associated_token_program,
            event_authority,
            program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            fee_config,
            fee_program,
            ..
        }
        => Some((
            PumpAmmSellAccounts {
                pool: *pool,
                user: *user,
                global_config: *global_config,
                base_mint: *base_mint,
                quote_mint: *quote_mint,
                user_base_token_account: *user_base_token_account,
                user_quote_token_account: *user_quote_token_account,
                pool_base_token_account: *pool_base_token_account,
                pool_quote_token_account: *pool_quote_token_account,
                protocol_fee_recipient: *protocol_fee_recipient,
                protocol_fee_recipient_token_account: *protocol_fee_recipient_token_account,
                base_token_program: *base_token_program,
                quote_token_program: *quote_token_program,
                system_program: *system_program,
                associated_token_program: *associated_token_program,
                event_authority: *event_authority,
                program: *program,
                coin_creator_vault_ata: *coin_creator_vault_ata,
                coin_creator_vault_authority: *coin_creator_vault_authority,
                fee_config: *fee_config,
                fee_program: *fee_program,
            },
            *base_mint,
        )),
        PumpAmmInstruction::Sell {
            pool,
            user,
            global_config,
            base_mint,
            quote_mint,
            user_base_token_account,
            user_quote_token_account,
            pool_base_token_account,
            pool_quote_token_account,
            protocol_fee_recipient,
            protocol_fee_recipient_token_account,
            base_token_program,
            quote_token_program,
            system_program,
            associated_token_program,
            event_authority,
            program,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            fee_config,
            fee_program,
            ..
        } => Some((
            PumpAmmSellAccounts {
                pool: *pool,
                user: *user,
                global_config: *global_config,
                base_mint: *base_mint,
                quote_mint: *quote_mint,
                user_base_token_account: *user_base_token_account,
                user_quote_token_account: *user_quote_token_account,
                pool_base_token_account: *pool_base_token_account,
                pool_quote_token_account: *pool_quote_token_account,
                protocol_fee_recipient: *protocol_fee_recipient,
                protocol_fee_recipient_token_account: *protocol_fee_recipient_token_account,
                base_token_program: *base_token_program,
                quote_token_program: *quote_token_program,
                system_program: *system_program,
                associated_token_program: *associated_token_program,
                event_authority: *event_authority,
                program: *program,
                coin_creator_vault_ata: *coin_creator_vault_ata,
                coin_creator_vault_authority: *coin_creator_vault_authority,
                fee_config: *fee_config,
                fee_program: *fee_program,
            },
            *base_mint,
        )),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TradeProtocol {
    PumpFun,
    PumpAmm,
}

impl TradeProtocol {
    fn cu_limit(&self, _observed_cost: Option<u64>) -> u32 {
        match self {
            TradeProtocol::PumpFun => PUMP_FUN_CU_LIMIT,
            TradeProtocol::PumpAmm => PUMP_AMM_CU_LIMIT,
        }
    }

    fn processors(&self) -> &'static [ProcessorEndpoint] {
        &[
            ProcessorEndpoint::Helius,
            ProcessorEndpoint::Astralane,
            ProcessorEndpoint::Blockrazor,
            ProcessorEndpoint::Stellium,
            ProcessorEndpoint::Flashblock,
            ProcessorEndpoint::ZeroSlot,
            ProcessorEndpoint::Nozomi,
            ProcessorEndpoint::StandardRpc,
        ]
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionSignal {
    pub protocol: TradeProtocol,
    pub target: Pubkey,
    pub slot: u64,
    pub sol_amount: Option<u64>,
    pub token_amount: Option<u64>,
    pub slippage_bps: u16,
    pub side: TradeSide,
    pub payload: ExecutionPayload,
    pub parsing_time_us: u64,
    pub created_at: Instant,
    pub observed_cu_consumed: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

impl ExecutionSignal {
    fn pump_fun_buy(
        target: TargetWalletConfig,
        slot: u64,
        sol: u64,
        token: u64,
        payload: ExecutionPayload,
        parsing_time_us: u64,
        observed_cu_consumed: Option<u64>,
    ) -> Self {
        Self {
            protocol: TradeProtocol::PumpFun,
            target: target.wallet,
            slot,
            sol_amount: Some(sol),
            token_amount: Some(token),
            slippage_bps: slippage_pct_to_bps(target.slippage_pct),
            side: TradeSide::Buy,
            payload,
            parsing_time_us,
            observed_cu_consumed,
            created_at: Instant::now(),
        }
    }

    fn pump_fun_sell(
        target: TargetWalletConfig,
        slot: u64,
        sol: u64,
        token: u64,
        payload: ExecutionPayload,
        parsing_time_us: u64,
        observed_cu_consumed: Option<u64>,
    ) -> Self {
        Self {
            protocol: TradeProtocol::PumpFun,
            target: target.wallet,
            slot,
            sol_amount: Some(sol),
            token_amount: Some(token),
            slippage_bps: slippage_pct_to_bps(target.slippage_pct),
            side: TradeSide::Sell,
            payload,
            parsing_time_us,
            observed_cu_consumed,
            created_at: Instant::now(),
        }
    }

    fn pump_amm_buy(
        target: TargetWalletConfig,
        slot: u64,
        sol_in: u64,
        base_out: u64,
        payload: ExecutionPayload,
        parsing_time_us: u64,
        observed_cu_consumed: Option<u64>,
    ) -> Self {
        Self {
            protocol: TradeProtocol::PumpAmm,
            target: target.wallet,
            slot,
            sol_amount: Some(sol_in),
            token_amount: Some(base_out),
            slippage_bps: slippage_pct_to_bps(target.slippage_pct),
            side: TradeSide::Buy,
            payload,
            parsing_time_us,
            observed_cu_consumed,
            created_at: Instant::now(),
        }
    }

    fn pump_amm_sell(
        target: TargetWalletConfig,
        slot: u64,
        sol_out: u64,
        base_in: u64,
        payload: ExecutionPayload,
        parsing_time_us: u64,
        observed_cu_consumed: Option<u64>,
    ) -> Self {
        Self {
            protocol: TradeProtocol::PumpAmm,
            target: target.wallet,
            slot,
            sol_amount: Some(sol_out),
            token_amount: Some(base_in),
            slippage_bps: slippage_pct_to_bps(target.slippage_pct),
            side: TradeSide::Sell,
            payload,
            parsing_time_us,
            observed_cu_consumed,
            created_at: Instant::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ExecutionPayload {
    PumpFunBuy(PumpFunBuyPayload),
    PumpFunSell(PumpFunSellPayload),
    PumpAmmBuy(PumpAmmBuyPayload),
    PumpAmmSell(PumpAmmSellPayload),
}

#[derive(Clone, Debug)]
pub struct PumpFunBuyPayload {
    pub accounts: BuyExactSolInAccounts,
    pub spendable_sol_in: u64,
    pub virtual_token_reserves: u128,
    pub virtual_sol_reserves: u128,
    pub real_token_reserves: u128,
    pub track_volume: bool,
}

#[derive(Clone, Debug)]
pub struct PumpFunSellPayload {
    pub accounts: PumpFunSellAccounts,
    pub amount: u64,
    pub virtual_token_reserves: u128,
    pub virtual_sol_reserves: u128,
}

#[derive(Clone, Debug)]
pub struct PumpAmmBuyPayload {
    pub accounts: BuyExactQuoteInAccounts,
    pub spendable_quote_in: u64,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub fees: Fees,
    pub track_volume: bool,
}

#[derive(Clone, Debug)]
pub struct PumpAmmSellPayload {
    pub accounts: PumpAmmSellAccounts,
    pub base_amount_in: u64,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub fees: Fees,
    pub expected_sol_out: u64,
}

pub struct PreparedExecution {
    pub protocol: TradeProtocol,
    pub target: Pubkey,
    pub slot: u64,
    pub nonce_slot: usize,
    pub plan: TxExecutionPlan,
    pub tips: Vec<(ProcessorEndpoint, Pubkey)>,
}

pub struct CopyTradeRuntime {
    #[allow(dead_code)]
    config: Arc<Config>,
    nonce_manager: Arc<NonceManager>,
    #[allow(dead_code)]
    executor: Arc<ExecutionPipeline>,
    confirmation_tracker: Arc<ConfirmationTracker>,
    next_slot: AtomicUsize,
}

#[derive(Clone, Copy, Debug)]
struct FeeSchedule {
    compute_unit_price_micro_lamports: u64,
    tip_lamports: u64,
}

impl FeeSchedule {
    fn matches_plan(self, plan: &TxExecutionPlan) -> bool {
        plan.compute_unit_price_micro_lamports == self.compute_unit_price_micro_lamports
            && plan.tip_lamports == self.tip_lamports
    }
}

/// Core instructions built once, reused across all processors.
#[derive(Clone)]
enum CoreInstructions {
    PumpFunBuy(PumpFunBuyCoreInstructions),
    PumpFunSell(PumpFunSellCoreInstructions),
    PumpAmmBuy(PumpAmmBuyCoreInstructions),
    PumpAmmSell(PumpAmmSellCoreInstructions),
}

impl CopyTradeRuntime {
    pub async fn execute(&self, signal: ExecutionSignal) -> Result<(), CopyTradeExecError> {
        let prepared = self.prepare_execution(&signal)?;
        debug!(
            "Prepared {:?} execution for target {} at slot {} using nonce slot {} (blockhash {}, cu_price={} µ-lamports, tip={} lamports)",
            signal.protocol,
            signal.target,
            signal.slot,
            prepared.nonce_slot,
            prepared.plan.nonce.blockhash,
            prepared.plan.compute_unit_price_micro_lamports,
            prepared.plan.tip_lamports
        );
        for (processor, tip) in &prepared.tips {
            debug!("Processor {} tip destination {}", processor.as_str(), tip);
        }
        self.dispatch(signal, prepared).await
    }

    pub fn new(
        config: Arc<Config>,
        nonce_manager: Arc<NonceManager>,
        executor: Arc<ExecutionPipeline>,
        confirmation_tracker: Arc<ConfirmationTracker>,
    ) -> Self {
        Self {
            config,
            nonce_manager,
            executor,
            confirmation_tracker,
            next_slot: AtomicUsize::new(0),
        }
    }

    pub fn prepare_execution(
        &self,
        signal: &ExecutionSignal,
    ) -> Result<PreparedExecution, CopyTradeExecError> {
        let cu_limit = signal.protocol.cu_limit(signal.observed_cu_consumed);
        let fee_schedule = self.fee_schedule_for_signal(signal, cu_limit);
        let (slot_index, plan) = self.reserve_plan(cu_limit, fee_schedule)?;

        // Filter processors to only those that are enabled (have API keys configured)
        let tips: Vec<_> = signal
            .protocol
            .processors()
            .iter()
            .filter(|processor| self.config.is_processor_enabled(processor.as_str()))
            .map(|processor| {
                (
                    *processor,
                    ExecutionPipeline::random_tip_address(*processor),
                )
            })
            .collect();

        if tips.is_empty() {
            warn!(
                "No transaction processors enabled for {:?} execution",
                signal.protocol
            );
            return Err(CopyTradeExecError::NoProcessorAccepted);
        }

        debug!(
            "Prepared execution with {} enabled processors: {:?}",
            tips.len(),
            tips.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>()
        );

        Ok(PreparedExecution {
            protocol: signal.protocol,
            target: signal.target,
            slot: signal.slot,
            nonce_slot: slot_index,
            plan,
            tips,
        })
    }

    pub fn handle_confirmed_transaction(&self, pending: &PendingConfirmation) {
        // Clear in-flight status first so the slot can be reused
        self.nonce_manager.clear_in_flight(pending.nonce_slot);

        // Wait for the nonce to advance on-chain before refreshing cache
        thread::sleep(Duration::from_millis(NONCE_REFRESH_DELAY_MS));

        // Refresh the blockhash cache for this slot
        if let Err(err) = self.nonce_manager.refresh_slot_cache(pending.nonce_slot) {
            warn!(
                "Failed to refresh nonce slot {} cache after {:?} confirmation: {err}",
                pending.nonce_slot, pending.protocol
            );
        } else {
            debug!(
                "Refreshed nonce slot {} cache after {:?} confirmation",
                pending.nonce_slot, pending.protocol
            );
        }
    }

    pub fn handle_expired_confirmation(&self, pending: PendingConfirmation) {
        warn!(
            "{:?} transaction timed out before confirmation (nonce slot {})",
            pending.protocol, pending.nonce_slot
        );

        // Clear in-flight status so the slot can be reused
        self.nonce_manager.clear_in_flight(pending.nonce_slot);

        // Try to refresh cache - the transaction might have landed without us seeing it
        if let Err(err) = self.nonce_manager.refresh_slot_cache(pending.nonce_slot) {
            warn!(
                "Failed to refresh nonce slot {} cache after timeout: {err}",
                pending.nonce_slot
            );
        }
    }

    async fn dispatch(
        &self,
        signal: ExecutionSignal,
        prepared: PreparedExecution,
    ) -> Result<(), CopyTradeExecError> {
        let operator = self.config.operator_keypair();
        let operator_pubkey = operator.pubkey();
        let cu_limit = signal.protocol.cu_limit(signal.observed_cu_consumed);

        let trade_mint = match &signal.payload {
            ExecutionPayload::PumpFunBuy(payload) => payload.accounts.mint,
            ExecutionPayload::PumpFunSell(payload) => payload.accounts.mint,
            ExecutionPayload::PumpAmmBuy(payload) => payload.accounts.base_mint,
            ExecutionPayload::PumpAmmSell(payload) => payload.accounts.base_mint,
        };

        let build_start = Instant::now();

        // Build core instructions once (expensive: math, borsh, ATA derivation)
        let core = Self::build_core_instructions(&signal, &operator_pubkey)?;

        // Assemble and sign all transactions in parallel (cheap per-processor)
        let built_txs: Vec<_> = prepared
            .tips
            .par_iter()
            .map(|(processor, tip_destination)| {
                let tx = Self::assemble_transaction_for_processor(
                    &core,
                    &prepared,
                    *processor,
                    *tip_destination,
                    &operator_pubkey,
                    &operator,
                    cu_limit,
                )?;
                let encoded = ExecutionPipeline::encode_versioned_transaction(&tx)
                    .map_err(CopyTradeExecError::Encoding)?;
                let signature = tx
                    .signatures
                    .first()
                    .ok_or(CopyTradeExecError::UnsupportedInstruction)?
                    .as_ref()
                    .to_vec();
                Ok((*processor, *tip_destination, tx, encoded, signature))
            })
            .collect::<Result<Vec<_>, CopyTradeExecError>>()?;

        let creation_duration = build_start.elapsed();

        // Sequential: record confirmations and build payloads
        let mut payloads = Vec::with_capacity(built_txs.len());
        for (processor, tip_destination, _tx, encoded, signature) in built_txs {
            let pending = PendingConfirmation {
                signature: signature.clone(),
                nonce_slot: prepared.nonce_slot,
                prepared_nonce: prepared.plan.nonce,
                cu_limit,
                protocol: signal.protocol,
                target: signal.target,
                mint: trade_mint,
                sent_at: Instant::now(),
            };
            self.confirmation_tracker.record(pending);
            let signature_str = bs58::encode(&signature).into_string();
            info_async!(
                "Dispatching {:?} tx {} for target {} mint {} via {} (nonce slot {}, tip dest {}, nonce hash {})",
                signal.protocol,
                signature_str,
                signal.target,
                trade_mint,
                processor.as_str(),
                prepared.nonce_slot,
                tip_destination,
                prepared.plan.nonce.blockhash
            );
            info_async!(
                "Tx {} base64 payload for {}: {}",
                signature_str,
                processor.as_str(),
                encoded
            );
            payloads.push((processor, encoded, signature, signature_str));
        }

        enum SendOutcome {
            Success {
                processor: ProcessorEndpoint,
                signature: String,
                send_duration: Duration,
                total_duration: Duration,
            },
            Failure {
                processor: ProcessorEndpoint,
                signature: String,
                error: ExecutionError,
                send_duration: Duration,
                total_duration: Duration,
            },
        }

        let futures = payloads
            .into_iter()
            .map(|(processor, encoded, signature, signature_str)| {
                let executor = Arc::clone(&self.executor);
                let tracker = Arc::clone(&self.confirmation_tracker);
                let created_at = signal.created_at;
                async move {
                    let send_start = Instant::now();
                    match executor.send_base64(processor, &encoded).await {
                        Ok(_) => SendOutcome::Success {
                            processor,
                            signature: signature_str.clone(),
                            send_duration: send_start.elapsed(),
                            total_duration: created_at.elapsed(),
                        },
                        Err(err) => {
                            tracker.remove_signature(&signature);
                            SendOutcome::Failure {
                                processor,
                                signature: signature_str.clone(),
                                error: err,
                                send_duration: send_start.elapsed(),
                                total_duration: created_at.elapsed(),
                            }
                        }
                    }
                }
            });

        let outcomes = future::join_all(futures).await;
        let mut success = false;
        for outcome in &outcomes {
            match outcome {
                SendOutcome::Success {
                    processor,
                    signature,
                    ..
                } => {
                    success = true;
                    info_async!(
                        "{} accepted mirrored transaction {}",
                        processor.as_str(),
                        signature
                    );
                }
                SendOutcome::Failure {
                    processor,
                    signature,
                    error,
                    ..
                } => {
                    warn!(
                        "{} sendTransaction failed for {}: {}",
                        processor.as_str(),
                        signature,
                        error
                    );
                }
            }
        }

        let mut profile_lines = vec![
            format!("Parsing Time: {} µs", signal.parsing_time_us),
            format!(
                "Transaction Creation + signing time: {} µs",
                creation_duration.as_micros()
            ),
        ];

        for outcome in &outcomes {
            match outcome {
                SendOutcome::Success {
                    processor,
                    send_duration,
                    total_duration,
                    ..
                } => profile_lines.push(format!(
                    "Sent to {} in {} ms (end-to-end {} ms)",
                    processor.as_str(),
                    send_duration.as_millis(),
                    total_duration.as_millis()
                )),
                SendOutcome::Failure {
                    processor,
                    send_duration,
                    total_duration,
                    error,
                    ..
                } => profile_lines.push(format!(
                    "Failed to send to {} after {} ms (end-to-end {} ms): {}",
                    processor.as_str(),
                    send_duration.as_millis(),
                    total_duration.as_millis(),
                    error
                )),
            }
        }

        info!(
            "{:?} profiling for target {}:\n    {}",
            signal.protocol,
            signal.target,
            profile_lines.join("\n    ")
        );

        if success {
            Ok(())
        } else {
            self.confirmation_tracker.cancel_slot(prepared.nonce_slot);
            self.restore_slot(prepared.nonce_slot);
            Err(CopyTradeExecError::NoProcessorAccepted)
        }
    }

    /// Build core instructions once (expensive: math, borsh, ATA derivation).
    fn build_core_instructions(
        signal: &ExecutionSignal,
        operator_pubkey: &Pubkey,
    ) -> Result<CoreInstructions, CopyTradeExecError> {
        match (&signal.payload, signal.protocol) {
            (ExecutionPayload::PumpFunBuy(payload), TradeProtocol::PumpFun) => {
                let mut accounts = payload.accounts.clone();
                retarget_pump_fun_buy_accounts(&mut accounts, operator_pubkey);
                let buy = pump::BuyExactSolInRequest {
                    accounts: &accounts,
                    spendable_sol_in: payload.spendable_sol_in,
                    virtual_token_reserves: payload.virtual_token_reserves,
                    virtual_sol_reserves: payload.virtual_sol_reserves,
                    real_token_reserves: payload.real_token_reserves,
                    slippage_bps: signal.slippage_bps,
                    track_volume: payload.track_volume,
                };
                let request = PumpFunBuyCoreRequest {
                    payer: *operator_pubkey,
                    ata_owner: *operator_pubkey,
                    ata_payer: *operator_pubkey,
                    token_program: accounts.token_program,
                    buy,
                };
                PumpFunTxBuilder::build_core_buy_instructions(request)
                    .map(CoreInstructions::PumpFunBuy)
                    .map_err(CopyTradeExecError::PumpFunBuilder)
            }
            (ExecutionPayload::PumpFunSell(payload), TradeProtocol::PumpFun) => {
                let mut accounts = payload.accounts.clone();
                retarget_pump_fun_sell_accounts(&mut accounts, operator_pubkey);
                let sell = pump::SellRequest {
                    accounts: &accounts,
                    amount: payload.amount,
                    virtual_token_reserves: payload.virtual_token_reserves,
                    virtual_sol_reserves: payload.virtual_sol_reserves,
                    slippage_bps: signal.slippage_bps,
                };
                let request = PumpFunSellCoreRequest {
                    payer: *operator_pubkey,
                    token_program: accounts.token_program,
                    sell,
                };
                PumpFunTxBuilder::build_core_sell_instructions(request)
                    .map(CoreInstructions::PumpFunSell)
                    .map_err(CopyTradeExecError::PumpFunBuilder)
            }
            (ExecutionPayload::PumpAmmBuy(payload), TradeProtocol::PumpAmm) => {
                let mut accounts = payload.accounts.clone();
                retarget_pump_amm_buy_accounts(&mut accounts, operator_pubkey);
                let buy = pump_amm::BuyExactQuoteInRequest {
                    accounts: &accounts,
                    spendable_quote_in: payload.spendable_quote_in,
                    base_reserve: payload.base_reserve,
                    quote_reserve: payload.quote_reserve,
                    fees: payload.fees,
                    slippage_bps: signal.slippage_bps,
                    track_volume: payload.track_volume,
                };
                let request = PumpAmmBuyCoreRequest {
                    payer: *operator_pubkey,
                    base_ata_owner: *operator_pubkey,
                    base_ata_payer: *operator_pubkey,
                    base_token_program: accounts.base_token_program,
                    buy,
                };
                PumpAmmTxBuilder::build_core_buy_instructions(request)
                    .map(CoreInstructions::PumpAmmBuy)
                    .map_err(CopyTradeExecError::PumpAmmBuilder)
            }
            (ExecutionPayload::PumpAmmSell(payload), TradeProtocol::PumpAmm) => {
                let mut accounts = payload.accounts.clone();
                retarget_pump_amm_sell_accounts(&mut accounts, operator_pubkey);
                let sell = pump_amm::SellRequest {
                    accounts: &accounts,
                    base_amount_in: payload.base_amount_in,
                    base_reserve: payload.base_reserve,
                    quote_reserve: payload.quote_reserve,
                    fees: payload.fees,
                    slippage_bps: signal.slippage_bps,
                };
                let request = PumpAmmSellCoreRequest {
                    payer: *operator_pubkey,
                    base_token_program: accounts.base_token_program,
                    sell,
                };
                PumpAmmTxBuilder::build_core_sell_instructions(request)
                    .map(CoreInstructions::PumpAmmSell)
                    .map_err(CopyTradeExecError::PumpAmmBuilder)
            }
            _ => Err(CopyTradeExecError::UnsupportedInstruction),
        }
    }

    /// Assemble full transaction for a processor (preamble + tip + core) and sign.
    fn assemble_transaction_for_processor<S: Signer>(
        core: &CoreInstructions,
        prepared: &PreparedExecution,
        processor: ProcessorEndpoint,
        tip_destination: Pubkey,
        operator_pubkey: &Pubkey,
        operator: &S,
        cu_limit: u32,
    ) -> Result<VersionedTransaction, CopyTradeExecError> {
        // Compute processor-specific fees
        let (compute_unit_price, tip_lamports) = if processor == ProcessorEndpoint::StandardRpc {
            let tip_as_priority = if cu_limit > 0 {
                (prepared.plan.tip_lamports as u128 * 1_000_000 / cu_limit as u128) as u64
            } else {
                0
            };
            (
                prepared
                    .plan
                    .compute_unit_price_micro_lamports
                    .saturating_add(tip_as_priority),
                0,
            )
        } else {
            (
                prepared.plan.compute_unit_price_micro_lamports,
                prepared.plan.tip_lamports,
            )
        };

        // Assemble instructions based on core type
        let instructions = match core {
            CoreInstructions::PumpFunBuy(core_buy) => {
                let preamble = PumpFunPreambleParams {
                    payer: *operator_pubkey,
                    nonce_account: prepared.plan.nonce.account,
                    nonce_authority: *operator_pubkey,
                    compute_unit_price_micro_lamports: compute_unit_price,
                    tip_destination,
                    tip_lamports,
                    cu_limit,
                };
                PumpFunTxBuilder::assemble_buy_with_preamble(&preamble, core_buy)
            }
            CoreInstructions::PumpFunSell(core_sell) => {
                let preamble = PumpFunPreambleParams {
                    payer: *operator_pubkey,
                    nonce_account: prepared.plan.nonce.account,
                    nonce_authority: *operator_pubkey,
                    compute_unit_price_micro_lamports: compute_unit_price,
                    tip_destination,
                    tip_lamports,
                    cu_limit,
                };
                PumpFunTxBuilder::assemble_sell_with_preamble(&preamble, core_sell)
            }
            CoreInstructions::PumpAmmBuy(core_buy) => {
                let preamble = PumpAmmPreambleParams {
                    payer: *operator_pubkey,
                    nonce_account: prepared.plan.nonce.account,
                    nonce_authority: *operator_pubkey,
                    compute_unit_price_micro_lamports: compute_unit_price,
                    tip_destination,
                    tip_lamports,
                    cu_limit,
                };
                PumpAmmTxBuilder::assemble_buy_with_preamble(&preamble, core_buy)
            }
            CoreInstructions::PumpAmmSell(core_sell) => {
                let preamble = PumpAmmPreambleParams {
                    payer: *operator_pubkey,
                    nonce_account: prepared.plan.nonce.account,
                    nonce_authority: *operator_pubkey,
                    compute_unit_price_micro_lamports: compute_unit_price,
                    tip_destination,
                    tip_lamports,
                    cu_limit,
                };
                PumpAmmTxBuilder::assemble_sell_with_preamble(&preamble, core_sell)
            }
        };

        // Build and sign the transaction
        let mut message = Message::new(&instructions, Some(operator_pubkey));
        message.recent_blockhash = prepared.plan.nonce.blockhash;
        VersionedTransaction::try_new(VersionedMessage::Legacy(message), &[operator])
            .map_err(CopyTradeExecError::Signing)
    }

    fn reserve_plan(
        &self,
        cu_limit: u32,
        fee_schedule: FeeSchedule,
    ) -> Result<(usize, TxExecutionPlan), NonceError> {
        // Round-robin slot selection, but skip in-flight slots
        let preferred = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.nonce_slots();
        let slot = self
            .nonce_manager
            .next_available_slot(preferred)
            .ok_or(NonceError::AllSlotsInFlight)?;

        // Create plan from cached blockhash (no RPC call)
        let plan = self.nonce_manager.plan_for_slot(
            slot,
            cu_limit,
            fee_schedule.compute_unit_price_micro_lamports,
            fee_schedule.tip_lamports,
        )?;

        // Mark slot as in-flight to prevent reuse before confirmation
        self.nonce_manager.mark_in_flight(slot);

        Ok((slot, plan))
    }

    fn nonce_slots(&self) -> usize {
        self.nonce_manager.nonce_accounts().len().max(1)
    }

    fn restore_slot(&self, slot: usize) {
        // Clear in-flight status so the slot can be reused
        self.nonce_manager.clear_in_flight(slot);
    }

    fn buy_fee_schedule(&self, cu_limit: u32) -> FeeSchedule {
        FeeSchedule {
            compute_unit_price_micro_lamports: self
                .config
                .buy_compute_unit_price_microlamports(cu_limit),
            tip_lamports: self.config.buy_tx_tip_lamports(),
        }
    }

    fn fee_schedule_for_signal(&self, signal: &ExecutionSignal, cu_limit: u32) -> FeeSchedule {
        match signal.side {
            TradeSide::Buy => FeeSchedule {
                compute_unit_price_micro_lamports: self
                    .config
                    .buy_compute_unit_price_microlamports(cu_limit),
                tip_lamports: self.config.buy_tx_tip_lamports(),
            },
            TradeSide::Sell => FeeSchedule {
                compute_unit_price_micro_lamports: self
                    .config
                    .sell_compute_unit_price_microlamports(cu_limit),
                tip_lamports: self.config.sell_tx_tip_lamports(),
            },
        }
    }
}

#[derive(Debug, Error)]
pub enum CopyTradeExecError {
    #[error("nonce planning failed: {0}")]
    Nonce(#[from] NonceError),
    #[error("pump fun builder error: {0}")]
    PumpFunBuilder(#[from] PumpTransactionBuilderError),
    #[error("pump amm builder error: {0}")]
    PumpAmmBuilder(#[from] PumpAmmBuilderError),
    #[error("transaction signing failed: {0}")]
    Signing(#[from] SignerError),
    #[error("encoding error: {0}")]
    Encoding(ExecutionError),
    #[error("no processor accepted transaction")]
    NoProcessorAccepted,
    #[error("unsupported instruction for execution")]
    UnsupportedInstruction,
}

#[derive(Clone)]
pub struct PendingConfirmation {
    pub signature: Vec<u8>,
    pub nonce_slot: usize,
    pub prepared_nonce: PreparedNonce,
    pub cu_limit: u32,
    pub protocol: TradeProtocol,
    pub target: Pubkey,
    pub mint: Pubkey,
    pub sent_at: Instant,
}

pub struct ConfirmationTracker {
    inner: Mutex<PendingMaps>,
    runtime: Mutex<Option<Weak<CopyTradeRuntime>>>,
}

struct PendingMaps {
    by_signature: HashMap<Vec<u8>, PendingConfirmation>,
    by_slot: HashMap<usize, HashSet<Vec<u8>>>,
}

impl ConfirmationTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(PendingMaps {
                by_signature: HashMap::new(),
                by_slot: HashMap::new(),
            }),
            runtime: Mutex::new(None),
        }
    }

    pub fn attach_runtime(&self, runtime: &Arc<CopyTradeRuntime>) {
        *self.runtime.lock().unwrap() = Some(Arc::downgrade(runtime));
    }

    pub fn record(self: &Arc<Self>, entry: PendingConfirmation) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard
                .by_slot
                .entry(entry.nonce_slot)
                .or_default()
                .insert(entry.signature.clone());
            guard.by_signature.insert(entry.signature.clone(), entry);
        }
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            sleep(Duration::from_secs(CONFIRMATION_TIMEOUT_SECS)).await;
            tracker.expire_overdue();
        });
    }

    fn expire_overdue(&self) {
        let expired_entries = {
            let mut guard = self.inner.lock().unwrap();
            let now = Instant::now();
            let expired_signatures: Vec<_> = guard
                .by_signature
                .iter()
                .filter_map(|(signature, entry)| {
                    if now.duration_since(entry.sent_at)
                        >= Duration::from_secs(CONFIRMATION_TIMEOUT_SECS)
                    {
                        Some(signature.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let mut removed = Vec::new();
            for signature in expired_signatures {
                if let Some(entry) = guard.by_signature.remove(&signature) {
                    if let Some(signatures) = guard.by_slot.get_mut(&entry.nonce_slot) {
                        signatures.retain(|sig| sig != &signature);
                        if signatures.is_empty() {
                            guard.by_slot.remove(&entry.nonce_slot);
                        }
                    }
                    removed.push(entry);
                }
            }
            removed
        };

        for entry in expired_entries {
            if let Some(runtime) = self
                .runtime
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|weak| weak.upgrade())
            {
                runtime.handle_expired_confirmation(entry);
            }
        }
    }

    pub fn remove_signature(&self, signature: &[u8]) -> Option<PendingConfirmation> {
        let mut guard = self.inner.lock().unwrap();
        let key = signature.to_vec();
        let entry = guard.by_signature.remove(&key)?;
        if let Some(signatures) = guard.by_slot.get_mut(&entry.nonce_slot) {
            signatures.retain(|sig| sig.as_slice() != signature);
            if signatures.is_empty() {
                guard.by_slot.remove(&entry.nonce_slot);
            }
        }
        Some(entry)
    }

    pub fn take(&self, signature: &[u8]) -> Option<PendingConfirmation> {
        let mut guard = self.inner.lock().unwrap();
        let key = signature.to_vec();
        let entry = guard.by_signature.remove(&key)?;
        if let Some(signatures) = guard.by_slot.remove(&entry.nonce_slot) {
            for sig in signatures {
                if sig.as_slice() != entry.signature.as_slice() {
                    guard.by_signature.remove(&sig);
                }
            }
        }
        Some(entry)
    }

    pub fn cancel_slot(&self, slot: usize) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(signatures) = guard.by_slot.remove(&slot) {
            for signature in signatures {
                guard.by_signature.remove(&signature);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::pump_amm::PumpAmmInstruction;
    use crate::parsers::pump_fun;
    use crate::parsers::pump_fun::{PumpFunInstruction, PumpFunOperation};

    #[test]
    fn trade_protocol_profiles() {
        assert_eq!(TradeProtocol::PumpFun.cu_limit(None), PUMP_FUN_CU_LIMIT);
        assert_eq!(TradeProtocol::PumpAmm.cu_limit(None), PUMP_AMM_CU_LIMIT);
        let pump_fun_processors = TradeProtocol::PumpFun.processors();
        assert_eq!(pump_fun_processors.len(), 8);
        assert_eq!(pump_fun_processors, TradeProtocol::PumpAmm.processors());
        assert!(pump_fun_processors.contains(&ProcessorEndpoint::Stellium));
        assert!(pump_fun_processors.contains(&ProcessorEndpoint::Flashblock));
        assert!(pump_fun_processors.contains(&ProcessorEndpoint::Nozomi));
    }

    #[test]
    fn proportional_sells_follow_example() {
        let mut ledger = MirrorLedger::default();
        let mint = Pubkey::new_unique();
        ledger.record_target_buy(mint, 1_000);
        ledger.record_operator_buy(mint, 100);

        let mut available = 100;
        let sell1 = ledger.plan_sell(mint, 250, available);
        assert_eq!(sell1, 25);
        available -= sell1;

        let sell2 = ledger.plan_sell(mint, 500, available);
        assert_eq!(sell2, 50);
        available -= sell2;

        let sell3 = ledger.plan_sell(mint, 250, available);
        assert_eq!(sell3, 25);
        available -= sell3;

        assert_eq!(available, 0);
    }

    #[test]
    fn pump_fun_operation_bundles_instruction_and_event() {
        // Test that PumpFunOperation correctly bundles instruction with event
        let new_key = || Pubkey::new_unique();
        let mint = new_key();
        let user = new_key();
        
        let instruction = PumpFunInstruction::Sell {
            amount: 1000,
            min_sol_output: 100,
            global: new_key(),
            fee_recipient: new_key(),
            mint,
            bonding_curve: new_key(),
            associated_bonding_curve: new_key(),
            associated_user: new_key(),
            user,
            system_program: new_key(),
            creator_vault: new_key(),
            token_program: new_key(),
            event_authority: new_key(),
            program: pump_fun::PUMP_FUN_PROGRAM_ID,
            fee_config: new_key(),
            fee_program: new_key(),
        };

        let event = PumpFunEvent::TradeEvent {
            mint,
            sol_amount: 100,
            token_amount: 1000,
            is_buy: false,
            user,
            timestamp: 0,
            virtual_sol_reserves: 1000000,
            virtual_token_reserves: 1000000,
            real_sol_reserves: 500000,
            real_token_reserves: 500000,
            fee_recipient: new_key(),
            fee_basis_points: 100,
            fee: 1,
            creator: new_key(),
            creator_fee_basis_points: 0,
            creator_fee: 0,
            track_volume: false,
            total_unclaimed_tokens: 0,
            total_claimed_tokens: 0,
            current_sol_volume: 0,
            last_update_timestamp: 0,
            ix_name: "sell".to_string(),
        };

        let operation = PumpFunOperation {
            instruction,
            event: Some(event),
        };

        // Verify the operation has both instruction and event
        assert!(operation.event.is_some());
        if let PumpFunInstruction::Sell { amount, .. } = &operation.instruction {
            assert_eq!(*amount, 1000);
        } else {
            panic!("Expected Sell instruction");
        }
    }

    #[test]
    fn pump_fun_payload_builds_from_operation() {
        let new_key = || Pubkey::new_unique();
        let mint = new_key();
        let user = new_key();
        
        let instruction = PumpFunInstruction::BuyExactSolIn {
            spendable_sol_in: 1000000,
            min_tokens_out: 100,
            track_volume: Some(true),
            global: new_key(),
            fee_recipient: new_key(),
            mint,
            bonding_curve: new_key(),
            associated_bonding_curve: new_key(),
            associated_user: new_key(),
            user,
            system_program: new_key(),
            token_program: new_key(),
            creator_vault: new_key(),
            event_authority: new_key(),
            program: pump_fun::PUMP_FUN_PROGRAM_ID,
            global_volume_accumulator: new_key(),
            user_volume_accumulator: new_key(),
            fee_config: new_key(),
            fee_program: new_key(),
        };

        let event = PumpFunEvent::TradeEvent {
            mint,
            sol_amount: 1000000,
            token_amount: 100,
            is_buy: true,
            user,
            timestamp: 0,
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_073_000_000_000_000,
            real_sol_reserves: 0,
            real_token_reserves: 793_100_000_000_000,
            fee_recipient: new_key(),
            fee_basis_points: 100,
            fee: 10000,
            creator: new_key(),
            creator_fee_basis_points: 0,
            creator_fee: 0,
            track_volume: true,
            total_unclaimed_tokens: 0,
            total_claimed_tokens: 0,
            current_sol_volume: 0,
            last_update_timestamp: 0,
            ix_name: "buy_exact_sol_in".to_string(),
        };

        let operation = PumpFunOperation {
            instruction,
            event: Some(event),
        };

        let payload = build_pump_fun_payload_from_operation(&operation);
        assert!(matches!(payload, Some(ExecutionPayload::PumpFunBuy(_))));
        
        if let Some(ExecutionPayload::PumpFunBuy(buy_payload)) = payload {
            assert_eq!(buy_payload.spendable_sol_in, 1000000);
            assert!(buy_payload.track_volume);
        }
    }

    #[test]
    fn pump_amm_payload_builds_from_operation() {
        use crate::parsers::pump_amm::{PumpAmmOperation, PUMP_AMM_PROGRAM_ID};
        
        let new_key = || Pubkey::new_unique();
        let pool = new_key();
        let user = new_key();
        let base_mint = new_key();
        let quote_mint = new_key();
        
        let instruction = PumpAmmInstruction::Sell {
            base_amount_in: 1000000,
            min_quote_amount_out: 100000,
            pool,
            user,
            global_config: new_key(),
            base_mint,
            quote_mint,
            user_base_token_account: new_key(),
            user_quote_token_account: new_key(),
            pool_base_token_account: new_key(),
            pool_quote_token_account: new_key(),
            protocol_fee_recipient: new_key(),
            protocol_fee_recipient_token_account: new_key(),
            base_token_program: new_key(),
            quote_token_program: new_key(),
            system_program: new_key(),
            associated_token_program: new_key(),
            event_authority: new_key(),
            program: PUMP_AMM_PROGRAM_ID,
            coin_creator_vault_ata: new_key(),
            coin_creator_vault_authority: new_key(),
            fee_config: new_key(),
            fee_program: new_key(),
        };

        let event = PumpAmmEvent::SellEvent {
            timestamp: 0,
            base_amount_in: 1000000,
            min_quote_amount_out: 100000,
            user_base_token_reserves: 5000000,
            user_quote_token_reserves: 1000000,
            pool_base_token_reserves: 206_900_000_000_000,
            pool_quote_token_reserves: 84_990_359_679,
            quote_amount_out: 100000,
            lp_fee_basis_points: 30,
            lp_fee: 30,
            protocol_fee_basis_points: 5,
            protocol_fee: 5,
            quote_amount_out_without_lp_fee: 99970,
            user_quote_amount_out: 99965,
            pool,
            user,
            user_base_token_account: new_key(),
            user_quote_token_account: new_key(),
            protocol_fee_recipient: new_key(),
            protocol_fee_recipient_token_account: new_key(),
            coin_creator: new_key(),
            coin_creator_fee_basis_points: 5,
            coin_creator_fee: 5,
        };

        let operation = PumpAmmOperation {
            instruction,
            event: Some(event),
        };

        let payload = build_pump_amm_sell_payload_from_operation(&operation);
        assert!(matches!(payload, Some(ExecutionPayload::PumpAmmSell(_))));
        
        if let Some(ExecutionPayload::PumpAmmSell(sell_payload)) = payload {
            assert_eq!(sell_payload.base_amount_in, 1000000);
            assert_eq!(sell_payload.fees.lp_fee_bps, 30);
            assert_eq!(sell_payload.fees.protocol_fee_bps, 5);
            assert_eq!(sell_payload.fees.coin_creator_fee_bps, 5);
        }
    }

    #[test]
    fn operator_total_tokens_sums_protocol_balances() {
        let mint = Pubkey::new_unique();
        let mut operator = OperatorState::new(Pubkey::new_unique());
        operator
            .pump_fun
            .update_token_position(mint, None, 1_000, DEFAULT_TOKEN_DECIMALS, 0);
        operator
            .pump_amm
            .update_token_position(mint, None, 500, DEFAULT_TOKEN_DECIMALS, 0);
        assert_eq!(operator.total_tokens(&mint), 1_500);
    }

    #[test]
    fn cross_positions_enable_mixed_protocol_sells() {
        let config = TargetWalletConfig {
            wallet: Pubkey::new_unique(),
            slippage_pct: 1.0,
            mirror_sells: true,
            take_profit_pct: None,
            stop_loss_pct: None,
        };
        let mut state = WalletTargetState::new(config);
        let mint = Pubkey::new_unique();
        state.pump_fun_positions.record_target_buy(mint, 1_000);
        state.mirror_positions.record_target_buy(mint, 1_000);
        state.pump_fun_positions.record_operator_buy(mint, 100);
        state.mirror_positions.record_operator_buy(mint, 100);
        assert_eq!(state.pump_amm_positions.plan_sell(mint, 500, 100), 0);
        let mirrored = state.mirror_positions.plan_sell(mint, 500, 100);
        assert_eq!(mirrored, 50);
    }
}
