#![allow(dead_code)]

use borsh::{self, BorshDeserialize, BorshSerialize};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey,
    pubkey::Pubkey,
    system_instruction,
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use std::io;
use thiserror::Error;

use crate::{
    parsers::pump_fun::{discriminators, PUMP_FUN_PROGRAM_ID},
    swap::constants::{
        get_buy_token_amount_from_sol_amount, get_sell_sol_amount_from_token_amount,
        LOADED_ACCOUNTS_DATA_LIMIT,
    },
};

const DEFAULT_CU_LIMIT: u32 = 120_000;
const MEV_GUARD_ACCOUNT: Pubkey = pubkey!("jitodontfrontB111111111111111111111SybauGng");

/// Builds Pump.fun buy/sell instructions while reusing account metas scraped from a
/// source transaction.
///
/// The builder is intentionally allocation-light (single Vec per IX) so it can
/// run in the hot copy-trading path.
pub struct PumpFunTransactionBuilder;

impl PumpFunTransactionBuilder {
    /// Build a `buy_exact_sol_in` instruction that applies slippage protection on
    /// the base-token amount (min_tokens_out).
    pub fn buy_exact_sol_in(
        params: BuyExactSolInRequest<'_>,
    ) -> Result<Instruction, PumpTransactionBuilderError> {
        ensure_program_id(params.accounts.program)?;

        let expected_tokens = get_buy_token_amount_from_sol_amount(
            params.virtual_token_reserves,
            params.virtual_sol_reserves,
            params.real_token_reserves,
            params.spendable_sol_in,
        );

        if expected_tokens == 0 {
            return Err(PumpTransactionBuilderError::ZeroExpectedTokens {
                spendable_sol_in: params.spendable_sol_in,
            });
        }

        let min_tokens_out = apply_base_slippage(expected_tokens, params.slippage_bps).ok_or(
            PumpTransactionBuilderError::SlippageTooLarge {
                slippage_bps: params.slippage_bps,
            },
        )?;

        let args = BuyExactSolInArgs {
            spendable_sol_in: params.spendable_sol_in,
            min_tokens_out,
            track_volume: OptionBool(params.track_volume),
        };

        let mut data = Vec::with_capacity(8 + core::mem::size_of::<BuyExactSolInArgs>());
        data.extend_from_slice(&discriminators::BUY_EXACT_SOL_IN);
        data.extend(borsh::to_vec(&args)?);

        Ok(Instruction {
            program_id: PUMP_FUN_PROGRAM_ID,
            accounts: buy_exact_sol_in_metas(params.accounts),
            data,
        })
    }

    /// Build a `sell` instruction using the standard Pump.fun curve math.
    pub fn sell(params: SellRequest<'_>) -> Result<Instruction, PumpTransactionBuilderError> {
        ensure_program_id(params.accounts.program)?;

        if params.amount == 0 {
            return Err(PumpTransactionBuilderError::ZeroSellOutput { amount: 0 });
        }

        let min_sol_output = get_sell_sol_amount_from_token_amount(
            params.virtual_token_reserves,
            params.virtual_sol_reserves,
            params.amount,
        );

        if min_sol_output == 0 {
            return Err(PumpTransactionBuilderError::ZeroSellOutput {
                amount: params.amount,
            });
        }
        let min_sol_output = apply_base_slippage(min_sol_output, params.slippage_bps).ok_or(
            PumpTransactionBuilderError::SlippageTooLarge {
                slippage_bps: params.slippage_bps,
            },
        )?;

        let args = SellArgs {
            amount: params.amount,
            min_sol_output,
        };

        let mut data = Vec::with_capacity(8 + core::mem::size_of::<SellArgs>());
        data.extend_from_slice(&discriminators::SELL);
        data.extend(borsh::to_vec(&args)?);

        Ok(Instruction {
            program_id: PUMP_FUN_PROGRAM_ID,
            accounts: sell_metas(params.accounts),
            data,
        })
    }
}

pub struct PumpFunTxBuilder;

/// Parameters for building the preamble (nonce, compute budget, tip).
#[derive(Clone, Debug)]
pub struct PumpFunPreambleParams {
    pub payer: Pubkey,
    pub nonce_account: Pubkey,
    pub nonce_authority: Pubkey,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_destination: Pubkey,
    pub tip_lamports: u64,
    pub cu_limit: u32,
}

/// Core buy instructions (ATA creation + swap) — built once, reused per processor.
#[derive(Clone, Debug)]
pub struct PumpFunBuyCoreInstructions {
    pub instructions: Vec<Instruction>,
}

/// Core sell instructions (just swap) — built once, reused per processor.
#[derive(Clone, Debug)]
pub struct PumpFunSellCoreInstructions {
    pub instructions: Vec<Instruction>,
}

/// Request to build core buy instructions (without preamble/tip).
#[derive(Clone, Debug)]
pub struct PumpFunBuyCoreRequest<'a> {
    pub payer: Pubkey,
    pub ata_owner: Pubkey,
    pub ata_payer: Pubkey,
    pub token_program: Pubkey,
    pub buy: BuyExactSolInRequest<'a>,
}

/// Request to build core sell instructions (without preamble/tip).
#[derive(Clone, Debug)]
pub struct PumpFunSellCoreRequest<'a> {
    pub payer: Pubkey,
    pub token_program: Pubkey,
    pub sell: SellRequest<'a>,
}

#[derive(Clone, Debug)]
pub struct PumpFunCommonTxParams {
    pub payer: Pubkey,
    pub nonce_account: Pubkey,
    pub nonce_authority: Pubkey,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_destination: Pubkey,
    pub tip_lamports: u64,
    pub cu_limit: u32,
    pub token_program: Pubkey,
}

impl PumpFunCommonTxParams {
    fn cu_limit(&self) -> u32 {
        if self.cu_limit == 0 {
            DEFAULT_CU_LIMIT
        } else {
            self.cu_limit
        }
    }

    fn validate(&self) -> Result<(), PumpTransactionBuilderError> {
        ensure_supported_token_program(self.token_program)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PumpFunBuyTxRequest<'a> {
    pub common: PumpFunCommonTxParams,
    pub ata_owner: Pubkey,
    pub ata_payer: Pubkey,
    pub buy: BuyExactSolInRequest<'a>,
}

#[derive(Clone, Debug)]
pub struct PumpFunSellTxRequest<'a> {
    pub common: PumpFunCommonTxParams,
    pub sell: SellRequest<'a>,
}

impl PumpFunTxBuilder {
    /// Build core buy instructions (ATA creation + swap) — expensive, call once.
    pub fn build_core_buy_instructions(
        request: PumpFunBuyCoreRequest<'_>,
    ) -> Result<PumpFunBuyCoreInstructions, PumpTransactionBuilderError> {
        ensure_supported_token_program(request.token_program)?;
        ensure_user_matches(request.payer, request.buy.accounts.user)?;
        ensure_token_program_matches(request.token_program, request.buy.accounts.token_program)?;

        let mut instructions = Vec::with_capacity(2);

        // ATA creation instruction
        let ata_ix = build_create_ata_core(&request)?;
        instructions.push(ata_ix);

        // Swap instruction with volume accumulator
        let mut buy_accounts = request.buy.accounts.clone();
        buy_accounts.user_volume_accumulator = user_volume_accumulator_pda(&request.payer);
        let buy_request = BuyExactSolInRequest {
            accounts: &buy_accounts,
            spendable_sol_in: request.buy.spendable_sol_in,
            virtual_token_reserves: request.buy.virtual_token_reserves,
            virtual_sol_reserves: request.buy.virtual_sol_reserves,
            real_token_reserves: request.buy.real_token_reserves,
            slippage_bps: request.buy.slippage_bps,
            track_volume: request.buy.track_volume,
        };
        let buy_ix = PumpFunTransactionBuilder::buy_exact_sol_in(buy_request)?;
        instructions.push(buy_ix);

        Ok(PumpFunBuyCoreInstructions { instructions })
    }

    /// Build core sell instructions (just swap) — expensive, call once.
    pub fn build_core_sell_instructions(
        request: PumpFunSellCoreRequest<'_>,
    ) -> Result<PumpFunSellCoreInstructions, PumpTransactionBuilderError> {
        ensure_supported_token_program(request.token_program)?;
        ensure_user_matches(request.payer, request.sell.accounts.user)?;
        ensure_token_program_matches(request.token_program, request.sell.accounts.token_program)?;

        let sell_ix = PumpFunTransactionBuilder::sell(request.sell)?;
        Ok(PumpFunSellCoreInstructions {
            instructions: vec![sell_ix],
        })
    }

    /// Assemble full buy transaction: preamble + tip + core instructions.
    pub fn assemble_buy_with_preamble(
        preamble: &PumpFunPreambleParams,
        core: &PumpFunBuyCoreInstructions,
    ) -> Vec<Instruction> {
        let has_tip = preamble.tip_lamports > 0;
        let capacity = 4 + if has_tip { 1 } else { 0 } + core.instructions.len();
        let mut instructions = Vec::with_capacity(capacity);

        instructions.push(advance_nonce_instruction(
            preamble.nonce_account,
            preamble.nonce_authority,
        ));
        instructions.push(compute_unit_limit_instruction(preamble.cu_limit));
        instructions.push(compute_unit_price_instruction(
            preamble.compute_unit_price_micro_lamports,
        ));
        instructions.push(
            ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                LOADED_ACCOUNTS_DATA_LIMIT,
            ),
        );

        if has_tip {
            instructions.push(tip_transfer_instruction(
                preamble.payer,
                preamble.tip_destination,
                preamble.tip_lamports,
            ));
        }

        instructions.extend(core.instructions.iter().cloned());
        instructions
    }

    /// Assemble full sell transaction: preamble + tip + core instructions.
    pub fn assemble_sell_with_preamble(
        preamble: &PumpFunPreambleParams,
        core: &PumpFunSellCoreInstructions,
    ) -> Vec<Instruction> {
        let has_tip = preamble.tip_lamports > 0;
        let capacity = 4 + if has_tip { 1 } else { 0 } + core.instructions.len();
        let mut instructions = Vec::with_capacity(capacity);

        instructions.push(advance_nonce_instruction(
            preamble.nonce_account,
            preamble.nonce_authority,
        ));
        instructions.push(compute_unit_limit_instruction(preamble.cu_limit));
        instructions.push(compute_unit_price_instruction(
            preamble.compute_unit_price_micro_lamports,
        ));
        instructions.push(
            ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                LOADED_ACCOUNTS_DATA_LIMIT,
            ),
        );

        if has_tip {
            instructions.push(tip_transfer_instruction(
                preamble.payer,
                preamble.tip_destination,
                preamble.tip_lamports,
            ));
        }

        instructions.extend(core.instructions.iter().cloned());
        instructions
    }

    /// Legacy: build complete buy transaction (for backward compatibility).
    pub fn build_buy_transaction(
        request: PumpFunBuyTxRequest<'_>,
    ) -> Result<Vec<Instruction>, PumpTransactionBuilderError> {
        let core_request = PumpFunBuyCoreRequest {
            payer: request.common.payer,
            ata_owner: request.ata_owner,
            ata_payer: request.ata_payer,
            token_program: request.common.token_program,
            buy: request.buy,
        };
        let core = Self::build_core_buy_instructions(core_request)?;
        let preamble = PumpFunPreambleParams {
            payer: request.common.payer,
            nonce_account: request.common.nonce_account,
            nonce_authority: request.common.nonce_authority,
            compute_unit_price_micro_lamports: request.common.compute_unit_price_micro_lamports,
            tip_destination: request.common.tip_destination,
            tip_lamports: request.common.tip_lamports,
            cu_limit: request.common.cu_limit(),
        };
        Ok(Self::assemble_buy_with_preamble(&preamble, &core))
    }

    /// Legacy: build complete sell transaction (for backward compatibility).
    pub fn build_sell_transaction(
        request: PumpFunSellTxRequest<'_>,
    ) -> Result<Vec<Instruction>, PumpTransactionBuilderError> {
        let core_request = PumpFunSellCoreRequest {
            payer: request.common.payer,
            token_program: request.common.token_program,
            sell: request.sell,
        };
        let core = Self::build_core_sell_instructions(core_request)?;
        let preamble = PumpFunPreambleParams {
            payer: request.common.payer,
            nonce_account: request.common.nonce_account,
            nonce_authority: request.common.nonce_authority,
            compute_unit_price_micro_lamports: request.common.compute_unit_price_micro_lamports,
            tip_destination: request.common.tip_destination,
            tip_lamports: request.common.tip_lamports,
            cu_limit: request.common.cu_limit(),
        };
        Ok(Self::assemble_sell_with_preamble(&preamble, &core))
    }
}

fn advance_nonce_instruction(nonce_account: Pubkey, nonce_authority: Pubkey) -> Instruction {
    system_instruction::advance_nonce_account(&nonce_account, &nonce_authority)
}

fn compute_unit_limit_instruction(cu_limit: u32) -> Instruction {
    let mut ix = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
    ix.accounts
        .push(AccountMeta::new_readonly(MEV_GUARD_ACCOUNT, false));
    ix
}

fn compute_unit_price_instruction(micro_lamports: u64) -> Instruction {
    ComputeBudgetInstruction::set_compute_unit_price(micro_lamports)
}

fn tip_transfer_instruction(payer: Pubkey, destination: Pubkey, lamports: u64) -> Instruction {
    system_instruction::transfer(&payer, &destination, lamports)
}

fn build_create_ata_instruction(
    request: &PumpFunBuyTxRequest<'_>,
    token_program: Pubkey,
) -> Result<Instruction, PumpTransactionBuilderError> {
    let derived = get_associated_token_address_with_program_id(
        &request.ata_owner,
        &request.buy.accounts.mint,
        &token_program,
    );
    if derived != request.buy.accounts.associated_user {
        return Err(PumpTransactionBuilderError::AssociatedUserMismatch {
            expected: request.buy.accounts.associated_user,
            derived,
        });
    }

    Ok(create_associated_token_account_idempotent(
        &request.ata_payer,
        &request.ata_owner,
        &request.buy.accounts.mint,
        &token_program,
    ))
}

fn build_create_ata_core(
    request: &PumpFunBuyCoreRequest<'_>,
) -> Result<Instruction, PumpTransactionBuilderError> {
    let derived = get_associated_token_address_with_program_id(
        &request.ata_owner,
        &request.buy.accounts.mint,
        &request.token_program,
    );
    if derived != request.buy.accounts.associated_user {
        return Err(PumpTransactionBuilderError::AssociatedUserMismatch {
            expected: request.buy.accounts.associated_user,
            derived,
        });
    }

    Ok(create_associated_token_account_idempotent(
        &request.ata_payer,
        &request.ata_owner,
        &request.buy.accounts.mint,
        &request.token_program,
    ))
}

fn ensure_token_program_matches(
    expected: Pubkey,
    provided: Pubkey,
) -> Result<(), PumpTransactionBuilderError> {
    if expected != provided {
        return Err(PumpTransactionBuilderError::TokenProgramMismatch { expected, provided });
    }
    Ok(())
}

fn ensure_user_matches(
    expected: Pubkey,
    provided: Pubkey,
) -> Result<(), PumpTransactionBuilderError> {
    if expected != provided {
        return Err(PumpTransactionBuilderError::UserMismatch { expected, provided });
    }
    Ok(())
}

fn ensure_supported_token_program(program: Pubkey) -> Result<(), PumpTransactionBuilderError> {
    if program == spl_token::id() || program == TOKEN_2022_PROGRAM_ID {
        Ok(())
    } else {
        Err(PumpTransactionBuilderError::UnsupportedTokenProgram { provided: program })
    }
}

/// Accounts required for `buy_exact_sol_in`.
#[derive(Clone, Debug)]
pub struct BuyExactSolInAccounts {
    pub global: Pubkey,
    pub fee_recipient: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub associated_user: Pubkey,
    pub user: Pubkey,
    pub system_program: Pubkey,
    pub token_program: Pubkey,
    pub creator_vault: Pubkey,
    pub event_authority: Pubkey,
    pub program: Pubkey,
    pub global_volume_accumulator: Pubkey,
    pub user_volume_accumulator: Pubkey,
    pub fee_config: Pubkey,
    pub fee_program: Pubkey,
}

/// Accounts required for `sell`.
#[derive(Clone, Debug)]
pub struct SellAccounts {
    pub global: Pubkey,
    pub fee_recipient: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub associated_user: Pubkey,
    pub user: Pubkey,
    pub system_program: Pubkey,
    pub creator_vault: Pubkey,
    pub token_program: Pubkey,
    pub event_authority: Pubkey,
    pub program: Pubkey,
    pub fee_config: Pubkey,
    pub fee_program: Pubkey,
}

/// Parameters for `buy_exact_sol_in`.
#[derive(Clone, Debug)]
pub struct BuyExactSolInRequest<'a> {
    pub accounts: &'a BuyExactSolInAccounts,
    pub spendable_sol_in: u64,
    pub virtual_token_reserves: u128,
    pub virtual_sol_reserves: u128,
    pub real_token_reserves: u128,
    pub slippage_bps: u16,
    pub track_volume: bool,
}

/// Parameters for `sell`.
#[derive(Clone, Debug)]
pub struct SellRequest<'a> {
    pub accounts: &'a SellAccounts,
    pub amount: u64,
    pub virtual_token_reserves: u128,
    pub virtual_sol_reserves: u128,
    pub slippage_bps: u16,
}

#[derive(Debug, Error)]
pub enum PumpTransactionBuilderError {
    #[error("expected token output is zero for spendable SOL {spendable_sol_in}")]
    ZeroExpectedTokens { spendable_sol_in: u64 },
    #[error("min tokens out became zero after applying {slippage_bps} bps slippage")]
    SlippageTooLarge { slippage_bps: u16 },
    #[error("sell output is zero for base amount {amount}")]
    ZeroSellOutput { amount: u64 },
    #[error("invalid Pump.fun program id {provided}")]
    InvalidProgramId { provided: Pubkey },
    #[error("priority fees must be non-negative, got {priority_fees}")]
    NegativePriorityFees { priority_fees: f64 },
    #[error("token program mismatch, expected {expected}, provided {provided}")]
    TokenProgramMismatch { expected: Pubkey, provided: Pubkey },
    #[error("transaction user {provided} does not match expected payer {expected}")]
    UserMismatch { expected: Pubkey, provided: Pubkey },
    #[error("unsupported token program {provided}")]
    UnsupportedTokenProgram { provided: Pubkey },
    #[error("associated user {expected} does not match derived ATA {derived}")]
    AssociatedUserMismatch { expected: Pubkey, derived: Pubkey },
    #[error(transparent)]
    Serialization(#[from] io::Error),
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BuyExactSolInArgs {
    spendable_sol_in: u64,
    min_tokens_out: u64,
    track_volume: OptionBool,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct SellArgs {
    amount: u64,
    min_sol_output: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct OptionBool(pub bool);

fn buy_exact_sol_in_metas(accounts: &BuyExactSolInAccounts) -> Vec<AccountMeta> {
    let mut metas = Vec::with_capacity(16);
    metas.push(AccountMeta::new_readonly(accounts.global, false));
    metas.push(AccountMeta::new(accounts.fee_recipient, false));
    metas.push(AccountMeta::new_readonly(accounts.mint, false));
    metas.push(AccountMeta::new(accounts.bonding_curve, false));
    metas.push(AccountMeta::new(accounts.associated_bonding_curve, false));
    metas.push(AccountMeta::new(accounts.associated_user, false));
    metas.push(AccountMeta::new(accounts.user, true));
    metas.push(AccountMeta::new_readonly(accounts.system_program, false));
    metas.push(AccountMeta::new_readonly(accounts.token_program, false));
    metas.push(AccountMeta::new(accounts.creator_vault, false));
    metas.push(AccountMeta::new_readonly(accounts.event_authority, false));
    metas.push(AccountMeta::new_readonly(accounts.program, false));
    metas.push(AccountMeta::new_readonly(
        accounts.global_volume_accumulator,
        false,
    ));
    metas.push(AccountMeta::new(accounts.user_volume_accumulator, false));
    metas.push(AccountMeta::new_readonly(accounts.fee_config, false));
    metas.push(AccountMeta::new_readonly(accounts.fee_program, false));
    metas
}

fn sell_metas(accounts: &SellAccounts) -> Vec<AccountMeta> {
    let mut metas = Vec::with_capacity(14);
    metas.push(AccountMeta::new_readonly(accounts.global, false));
    metas.push(AccountMeta::new(accounts.fee_recipient, false));
    metas.push(AccountMeta::new_readonly(accounts.mint, false));
    metas.push(AccountMeta::new(accounts.bonding_curve, false));
    metas.push(AccountMeta::new(accounts.associated_bonding_curve, false));
    metas.push(AccountMeta::new(accounts.associated_user, false));
    metas.push(AccountMeta::new(accounts.user, true));
    metas.push(AccountMeta::new_readonly(accounts.system_program, false));
    metas.push(AccountMeta::new(accounts.creator_vault, false));
    metas.push(AccountMeta::new_readonly(accounts.token_program, false));
    metas.push(AccountMeta::new_readonly(accounts.event_authority, false));
    metas.push(AccountMeta::new_readonly(accounts.program, false));
    metas.push(AccountMeta::new_readonly(accounts.fee_config, false));
    metas.push(AccountMeta::new_readonly(accounts.fee_program, false));
    metas
}

fn ensure_program_id(program: Pubkey) -> Result<(), PumpTransactionBuilderError> {
    if program != PUMP_FUN_PROGRAM_ID {
        return Err(PumpTransactionBuilderError::InvalidProgramId { provided: program });
    }
    Ok(())
}

fn apply_base_slippage(amount: u64, slippage_bps: u16) -> Option<u64> {
    if amount == 0 {
        return None;
    }
    let capped_bps = slippage_bps.min(10_000);
    if capped_bps == 10_000 {
        return None;
    }
    let numerator = (amount as u128).saturating_mul((10_000u32 - capped_bps as u32) as u128);
    let adjusted = (numerator / 10_000u128) as u64;
    if adjusted == 0 {
        None
    } else {
        Some(adjusted)
    }
}

pub fn user_volume_accumulator_pda(user: &Pubkey) -> Pubkey {
    let (user_volume_accumulator, _bump) = Pubkey::find_program_address(
        &[b"user_volume_accumulator", user.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    );
    user_volume_accumulator
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::BorshDeserialize;
    use solana_sdk::{compute_budget, system_program};

    fn random_pubkey() -> Pubkey {
        Pubkey::new_unique()
    }

    fn sample_buy_accounts(
        user: Pubkey,
        mint: Pubkey,
        token_program: Pubkey,
    ) -> BuyExactSolInAccounts {
        let associated_user =
            get_associated_token_address_with_program_id(&user, &mint, &token_program);
        BuyExactSolInAccounts {
            global: random_pubkey(),
            fee_recipient: random_pubkey(),
            mint,
            bonding_curve: random_pubkey(),
            associated_bonding_curve: random_pubkey(),
            associated_user,
            user,
            system_program: system_program::id(),
            token_program,
            creator_vault: random_pubkey(),
            event_authority: random_pubkey(),
            program: PUMP_FUN_PROGRAM_ID,
            global_volume_accumulator: random_pubkey(),
            user_volume_accumulator: random_pubkey(),
            fee_config: random_pubkey(),
            fee_program: random_pubkey(),
        }
    }

    fn sample_sell_accounts(user: Pubkey, mint: Pubkey, token_program: Pubkey) -> SellAccounts {
        SellAccounts {
            global: random_pubkey(),
            fee_recipient: random_pubkey(),
            mint,
            bonding_curve: random_pubkey(),
            associated_bonding_curve: random_pubkey(),
            associated_user: get_associated_token_address_with_program_id(
                &user,
                &mint,
                &token_program,
            ),
            user,
            system_program: system_program::id(),
            creator_vault: random_pubkey(),
            token_program,
            event_authority: random_pubkey(),
            program: PUMP_FUN_PROGRAM_ID,
            fee_config: random_pubkey(),
            fee_program: random_pubkey(),
        }
    }

    #[test]
    fn build_buy_transaction_orders_instructions_for_legacy_token() {
        let payer = random_pubkey();
        let mint = random_pubkey();
        let token_program = spl_token::id();
        let buy_accounts = sample_buy_accounts(payer, mint, token_program);
        let buy_request = BuyExactSolInRequest {
            accounts: &buy_accounts,
            spendable_sol_in: 1_000_000_000,
            virtual_token_reserves: 1_000_000_000,
            virtual_sol_reserves: 1_000_000_000,
            real_token_reserves: 1_000_000_000,
            slippage_bps: 100,
            track_volume: true,
        };

        let common = PumpFunCommonTxParams {
            payer,
            nonce_account: random_pubkey(),
            nonce_authority: payer,
            compute_unit_price_micro_lamports: 1_000,
            tip_destination: random_pubkey(),
            tip_lamports: 5_000,
            cu_limit: DEFAULT_CU_LIMIT,
            token_program,
        };

        let request = PumpFunBuyTxRequest {
            common,
            ata_owner: payer,
            ata_payer: payer,
            buy: buy_request,
        };

        let instructions = PumpFunTxBuilder::build_buy_transaction(request).unwrap();
        assert_eq!(instructions.len(), 7);
        assert_eq!(instructions[0].program_id, system_program::id());
        assert_eq!(instructions[1].program_id, compute_budget::id());
        assert_eq!(
            instructions[1].accounts.last().unwrap().pubkey,
            MEV_GUARD_ACCOUNT
        );
        assert_eq!(instructions[2].program_id, compute_budget::id());
        assert_eq!(instructions[3].program_id, compute_budget::id());
        assert_eq!(instructions[4].program_id, system_program::id());
        assert_eq!(
            instructions[5].program_id,
            spl_associated_token_account::id()
        );
        assert_eq!(instructions[6].program_id, PUMP_FUN_PROGRAM_ID);

        let derived_uva = user_volume_accumulator_pda(&payer);
        let pump_ix = instructions.last().unwrap();
        assert_eq!(pump_ix.accounts[13].pubkey, derived_uva);
        let args = BuyExactSolInArgs::try_from_slice(&pump_ix.data[8..]).expect("borsh decode");
        assert_eq!(args.spendable_sol_in, 1_000_000_000);
    }

    #[test]
    fn build_buy_transaction_supports_token_2022() {
        let payer = random_pubkey();
        let mint = random_pubkey();
        let token_program = TOKEN_2022_PROGRAM_ID;
        let buy_accounts = sample_buy_accounts(payer, mint, token_program);
        let buy_request = BuyExactSolInRequest {
            accounts: &buy_accounts,
            spendable_sol_in: 500_000_000,
            virtual_token_reserves: 2_000_000_000,
            virtual_sol_reserves: 1_500_000_000,
            real_token_reserves: 2_000_000_000,
            slippage_bps: 50,
            track_volume: false,
        };

        let common = PumpFunCommonTxParams {
            payer,
            nonce_account: random_pubkey(),
            nonce_authority: payer,
            compute_unit_price_micro_lamports: 1_000,
            tip_destination: random_pubkey(),
            tip_lamports: 10_000,
            cu_limit: DEFAULT_CU_LIMIT,
            token_program,
        };

        let request = PumpFunBuyTxRequest {
            common,
            ata_owner: payer,
            ata_payer: payer,
            buy: buy_request,
        };

        let instructions = PumpFunTxBuilder::build_buy_transaction(request).unwrap();
        assert_eq!(instructions.len(), 7);
        assert_eq!(
            instructions[5].program_id,
            spl_associated_token_account::id()
        );
    }

    #[test]
    fn build_sell_transaction_omits_ata_instruction() {
        let payer = random_pubkey();
        let mint = random_pubkey();
        let token_program = spl_token::id();
        let sell_accounts = sample_sell_accounts(payer, mint, token_program);
        let sell_request = SellRequest {
            accounts: &sell_accounts,
            amount: 1_000_000,
            virtual_token_reserves: 2_000_000_000,
            virtual_sol_reserves: 2_500_000_000,
            slippage_bps: 0,
        };

        let common = PumpFunCommonTxParams {
            payer,
            nonce_account: random_pubkey(),
            nonce_authority: payer,
            compute_unit_price_micro_lamports: 1_000,
            tip_destination: random_pubkey(),
            tip_lamports: 12_345,
            cu_limit: DEFAULT_CU_LIMIT,
            token_program,
        };

        let request = PumpFunSellTxRequest {
            common,
            sell: sell_request,
        };
        let instructions = PumpFunTxBuilder::build_sell_transaction(request).unwrap();
        assert_eq!(instructions.len(), 6);
        assert_eq!(instructions[0].program_id, system_program::id());
        assert_eq!(instructions[1].program_id, compute_budget::id());
        assert_eq!(
            instructions[1].accounts.last().unwrap().pubkey,
            MEV_GUARD_ACCOUNT
        );
        assert_eq!(instructions[2].program_id, compute_budget::id());
        assert_eq!(instructions[3].program_id, compute_budget::id());
        assert_eq!(instructions[4].program_id, system_program::id());
        assert_eq!(instructions[5].program_id, PUMP_FUN_PROGRAM_ID);
    }
}
