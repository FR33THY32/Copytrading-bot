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
use spl_token::instruction::{close_account, sync_native};
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use thiserror::Error;

use crate::swap::constants::{
    buy_quote_input_internal, sell_base_input_internal, Fees, LOADED_ACCOUNTS_DATA_LIMIT,
};

const PUMP_AMM_PROGRAM_ID: Pubkey = pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
const DEFAULT_CU_LIMIT: u32 = 200_000;
const MEV_GUARD_ACCOUNT: Pubkey = pubkey!("jitodontfrontB111111111111111111111SybauGng");
const LEGACY_TOKEN_PROGRAM: Pubkey = spl_token::id();

/// Thin, allocation-aware builder for Pump AMM buy/sell instructions.
pub struct PumpAmmTransactionBuilder;

impl PumpAmmTransactionBuilder {
    pub fn buy_exact_quote_in(
        params: BuyExactQuoteInRequest<'_>,
    ) -> Result<Instruction, PumpAmmBuilderError> {
        ensure_program_id(params.accounts.program)?;

        let quote = params.spendable_quote_in;
        if quote == 0 {
            return Err(PumpAmmBuilderError::ZeroQuote);
        }

        let buy_result = buy_quote_input_internal(
            quote,
            0, // quote-side slippage handled via min_base_out
            params.base_reserve,
            params.quote_reserve,
            params.fees,
        )
        .map_err(PumpAmmBuilderError::Math)?;

        let expected_base = buy_result.base;
        if expected_base == 0 {
            return Err(PumpAmmBuilderError::ZeroBaseAfterMath);
        }

        let min_base_amount_out = apply_base_slippage(expected_base, params.slippage_bps).ok_or(
            PumpAmmBuilderError::SlippageTooLarge {
                slippage_bps: params.slippage_bps,
            },
        )?;

        let args = BuyExactQuoteInArgs {
            spendable_quote_in: quote,
            min_base_amount_out,
            track_volume: OptionBool(params.track_volume),
        };

        let mut data = Vec::with_capacity(8 + core::mem::size_of::<BuyExactQuoteInArgs>());
        data.extend_from_slice(&pump_amm_discriminators::BUY_EXACT_QUOTE_IN);
        data.extend(borsh::to_vec(&args)?);

        Ok(Instruction {
            program_id: PUMP_AMM_PROGRAM_ID,
            accounts: buy_exact_quote_in_metas(params.accounts),
            data,
        })
    }

    pub fn sell(params: SellRequest<'_>) -> Result<Instruction, PumpAmmBuilderError> {
        ensure_program_id(params.accounts.program)?;

        if params.base_amount_in == 0 {
            return Err(PumpAmmBuilderError::ZeroBaseInput);
        }

        let sell_result = sell_base_input_internal(
            params.base_amount_in,
            params.slippage_bps as u64,
            params.base_reserve,
            params.quote_reserve,
            params.fees,
        )
        .map_err(PumpAmmBuilderError::Math)?;

        if sell_result.ui_quote == 0 {
            return Err(PumpAmmBuilderError::ZeroQuoteAfterMath);
        }

        let args = SellArgs {
            base_amount_in: params.base_amount_in,
            min_quote_amount_out: sell_result.min_quote,
        };

        let mut data = Vec::with_capacity(8 + core::mem::size_of::<SellArgs>());
        data.extend_from_slice(&pump_amm_discriminators::SELL);
        data.extend(borsh::to_vec(&args)?);

        Ok(Instruction {
            program_id: PUMP_AMM_PROGRAM_ID,
            accounts: sell_metas(params.accounts),
            data,
        })
    }
}

/// Accounts required for `buy_exact_quote_in`.
#[derive(Clone, Debug)]
pub struct BuyExactQuoteInAccounts {
    pub pool: Pubkey,
    pub user: Pubkey,
    pub global_config: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub user_base_token_account: Pubkey,
    pub user_quote_token_account: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub protocol_fee_recipient: Pubkey,
    pub protocol_fee_recipient_token_account: Pubkey,
    pub base_token_program: Pubkey,
    pub quote_token_program: Pubkey,
    pub system_program: Pubkey,
    pub associated_token_program: Pubkey,
    pub event_authority: Pubkey,
    pub program: Pubkey,
    pub coin_creator_vault_ata: Pubkey,
    pub coin_creator_vault_authority: Pubkey,
    pub global_volume_accumulator: Pubkey,
    pub user_volume_accumulator: Pubkey,
    pub fee_config: Pubkey,
    pub fee_program: Pubkey,
}

/// Accounts required for `sell`.
#[derive(Clone, Debug)]
pub struct SellAccounts {
    pub pool: Pubkey,
    pub user: Pubkey,
    pub global_config: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub user_base_token_account: Pubkey,
    pub user_quote_token_account: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub protocol_fee_recipient: Pubkey,
    pub protocol_fee_recipient_token_account: Pubkey,
    pub base_token_program: Pubkey,
    pub quote_token_program: Pubkey,
    pub system_program: Pubkey,
    pub associated_token_program: Pubkey,
    pub event_authority: Pubkey,
    pub program: Pubkey,
    pub coin_creator_vault_ata: Pubkey,
    pub coin_creator_vault_authority: Pubkey,
    pub fee_config: Pubkey,
    pub fee_program: Pubkey,
}

#[derive(Clone, Debug)]
pub struct BuyExactQuoteInRequest<'a> {
    pub accounts: &'a BuyExactQuoteInAccounts,
    pub spendable_quote_in: u64,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub fees: Fees,
    pub slippage_bps: u16,
    pub track_volume: bool,
}

#[derive(Clone, Debug)]
pub struct SellRequest<'a> {
    pub accounts: &'a SellAccounts,
    pub base_amount_in: u64,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub fees: Fees,
    pub slippage_bps: u16,
}

#[derive(Debug, Error)]
pub enum PumpAmmBuilderError {
    #[error("slippage {slippage_bps} bps reduced base to zero")]
    SlippageTooLarge { slippage_bps: u16 },
    #[error("quote spend cannot be zero")]
    ZeroQuote,
    #[error("base amount after math is zero")]
    ZeroBaseAfterMath,
    #[error("base input cannot be zero")]
    ZeroBaseInput,
    #[error("quote output after math is zero")]
    ZeroQuoteAfterMath,
    #[error("math error: {0}")]
    Math(String),
    #[error("invalid Pump AMM program id {provided}")]
    InvalidProgramId { provided: Pubkey },
    #[error("token program mismatch, expected {expected}, provided {provided}")]
    TokenProgramMismatch { expected: Pubkey, provided: Pubkey },
    #[error("transaction user {provided} does not match expected payer {expected}")]
    UserMismatch { expected: Pubkey, provided: Pubkey },
    #[error("unsupported token program {provided}")]
    UnsupportedTokenProgram { provided: Pubkey },
    #[error("derived ATA {derived} does not match expected base account {expected}")]
    AssociatedBaseAccountMismatch { expected: Pubkey, derived: Pubkey },
    #[error("derived ATA {derived} does not match expected quote account {expected}")]
    AssociatedQuoteAccountMismatch { expected: Pubkey, derived: Pubkey },
    #[error("token instruction error: {0}")]
    TokenInstruction(String),
    #[error(transparent)]
    Serialization(#[from] std::io::Error),
}

mod pump_amm_discriminators {
    pub const BUY_EXACT_QUOTE_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];
    pub const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BuyExactQuoteInArgs {
    spendable_quote_in: u64,
    min_base_amount_out: u64,
    track_volume: OptionBool,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct SellArgs {
    base_amount_in: u64,
    min_quote_amount_out: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct OptionBool(pub bool);

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

fn create_base_ata_instruction(
    payer: Pubkey,
    owner: Pubkey,
    mint: Pubkey,
    token_program: Pubkey,
) -> Instruction {
    create_associated_token_account_idempotent(&payer, &owner, &mint, &token_program)
}

fn create_wsol_ata_instruction(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> Instruction {
    create_associated_token_account_idempotent(&payer, &owner, &mint, &LEGACY_TOKEN_PROGRAM)
}

fn transfer_sol_to_ata_instruction(payer: Pubkey, ata: Pubkey, lamports: u64) -> Instruction {
    system_instruction::transfer(&payer, &ata, lamports)
}

fn sync_native_instruction(account: Pubkey) -> Result<Instruction, PumpAmmBuilderError> {
    sync_native(&LEGACY_TOKEN_PROGRAM, &account)
        .map_err(|e| PumpAmmBuilderError::TokenInstruction(e.to_string()))
}

fn close_wsol_instruction(
    account: Pubkey,
    destination: Pubkey,
    authority: Pubkey,
) -> Result<Instruction, PumpAmmBuilderError> {
    close_account(
        &LEGACY_TOKEN_PROGRAM,
        &account,
        &destination,
        &authority,
        &[],
    )
    .map_err(|e| PumpAmmBuilderError::TokenInstruction(e.to_string()))
}

fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(owner, mint, token_program)
}

fn ensure_token_program_matches(
    expected: Pubkey,
    provided: Pubkey,
) -> Result<(), PumpAmmBuilderError> {
    if expected != provided {
        return Err(PumpAmmBuilderError::TokenProgramMismatch { expected, provided });
    }
    Ok(())
}

fn ensure_supported_token_program(program: Pubkey) -> Result<(), PumpAmmBuilderError> {
    if program == LEGACY_TOKEN_PROGRAM || program == TOKEN_2022_PROGRAM_ID {
        Ok(())
    } else {
        Err(PumpAmmBuilderError::UnsupportedTokenProgram { provided: program })
    }
}

fn ensure_quote_program_is_legacy(program: Pubkey) -> Result<(), PumpAmmBuilderError> {
    if program == LEGACY_TOKEN_PROGRAM {
        Ok(())
    } else {
        Err(PumpAmmBuilderError::TokenProgramMismatch {
            expected: LEGACY_TOKEN_PROGRAM,
            provided: program,
        })
    }
}

fn ensure_user_matches(expected: Pubkey, provided: Pubkey) -> Result<(), PumpAmmBuilderError> {
    if expected != provided {
        return Err(PumpAmmBuilderError::UserMismatch { expected, provided });
    }
    Ok(())
}

pub struct PumpAmmTxBuilder;

/// Parameters for building the preamble (nonce, compute budget, tip).
#[derive(Clone, Debug)]
pub struct PumpAmmPreambleParams {
    pub payer: Pubkey,
    pub nonce_account: Pubkey,
    pub nonce_authority: Pubkey,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_destination: Pubkey,
    pub tip_lamports: u64,
    pub cu_limit: u32,
}

/// Core buy instructions (ATA creation, WSOL wrap, swap, WSOL close) — built once, reused per processor.
#[derive(Clone, Debug)]
pub struct PumpAmmBuyCoreInstructions {
    pub instructions: Vec<Instruction>,
}

/// Core sell instructions (WSOL ATA, swap, WSOL close) — built once, reused per processor.
#[derive(Clone, Debug)]
pub struct PumpAmmSellCoreInstructions {
    pub instructions: Vec<Instruction>,
}

/// Request to build core buy instructions (without preamble/tip).
#[derive(Clone, Debug)]
pub struct PumpAmmBuyCoreRequest<'a> {
    pub payer: Pubkey,
    pub base_ata_owner: Pubkey,
    pub base_ata_payer: Pubkey,
    pub base_token_program: Pubkey,
    pub buy: BuyExactQuoteInRequest<'a>,
}

/// Request to build core sell instructions (without preamble/tip).
#[derive(Clone, Debug)]
pub struct PumpAmmSellCoreRequest<'a> {
    pub payer: Pubkey,
    pub base_token_program: Pubkey,
    pub sell: SellRequest<'a>,
}

#[derive(Clone, Debug)]
pub struct PumpAmmCommonTxParams {
    pub payer: Pubkey,
    pub nonce_account: Pubkey,
    pub nonce_authority: Pubkey,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_destination: Pubkey,
    pub tip_lamports: u64,
    pub cu_limit: u32,
    pub base_token_program: Pubkey,
}

impl PumpAmmCommonTxParams {
    fn cu_limit(&self) -> u32 {
        if self.cu_limit == 0 {
            DEFAULT_CU_LIMIT
        } else {
            self.cu_limit
        }
    }

    fn validate(&self) -> Result<(), PumpAmmBuilderError> {
        ensure_supported_token_program(self.base_token_program)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PumpAmmBuyTxRequest<'a> {
    pub common: PumpAmmCommonTxParams,
    pub base_ata_owner: Pubkey,
    pub base_ata_payer: Pubkey,
    pub buy: BuyExactQuoteInRequest<'a>,
}

#[derive(Clone, Debug)]
pub struct PumpAmmSellTxRequest<'a> {
    pub common: PumpAmmCommonTxParams,
    pub quote_wrap_amount: u64,
    pub sell: SellRequest<'a>,
}

impl PumpAmmTxBuilder {
    /// Build core buy instructions (ATA creation, WSOL wrap, swap, close) — expensive, call once.
    pub fn build_core_buy_instructions(
        request: PumpAmmBuyCoreRequest<'_>,
    ) -> Result<PumpAmmBuyCoreInstructions, PumpAmmBuilderError> {
        ensure_supported_token_program(request.base_token_program)?;
        ensure_user_matches(request.payer, request.buy.accounts.user)?;

        let mut accounts = request.buy.accounts.clone();
        accounts.user_volume_accumulator = user_volume_accumulator_pda(&request.payer);

        ensure_token_program_matches(request.base_token_program, accounts.base_token_program)?;
        ensure_quote_program_is_legacy(accounts.quote_token_program)?;

        let base_ata = derive_ata(
            &request.base_ata_owner,
            &accounts.base_mint,
            &request.base_token_program,
        );
        if base_ata != accounts.user_base_token_account {
            return Err(PumpAmmBuilderError::AssociatedBaseAccountMismatch {
                expected: accounts.user_base_token_account,
                derived: base_ata,
            });
        }

        let quote_ata = derive_ata(&accounts.user, &accounts.quote_mint, &LEGACY_TOKEN_PROGRAM);
        if quote_ata != accounts.user_quote_token_account {
            return Err(PumpAmmBuilderError::AssociatedQuoteAccountMismatch {
                expected: accounts.user_quote_token_account,
                derived: quote_ata,
            });
        }

        let buy_user = accounts.user;
        let mut instructions = Vec::with_capacity(6);

        // Base ATA creation
        instructions.push(create_base_ata_instruction(
            request.base_ata_payer,
            request.base_ata_owner,
            accounts.base_mint,
            request.base_token_program,
        ));
        // WSOL ATA creation
        instructions.push(create_wsol_ata_instruction(
            request.payer,
            accounts.user,
            accounts.quote_mint,
        ));
        // Transfer SOL to WSOL ATA
        instructions.push(transfer_sol_to_ata_instruction(
            request.payer,
            quote_ata,
            request.buy.spendable_quote_in,
        ));
        // Sync native
        instructions.push(sync_native_instruction(quote_ata)?);

        // Swap instruction
        let buy_request = BuyExactQuoteInRequest {
            accounts: &accounts,
            spendable_quote_in: request.buy.spendable_quote_in,
            base_reserve: request.buy.base_reserve,
            quote_reserve: request.buy.quote_reserve,
            fees: request.buy.fees,
            slippage_bps: request.buy.slippage_bps,
            track_volume: request.buy.track_volume,
        };
        instructions.push(PumpAmmTransactionBuilder::buy_exact_quote_in(buy_request)?);

        // Close WSOL ATA
        instructions.push(close_wsol_instruction(quote_ata, request.payer, buy_user)?);

        Ok(PumpAmmBuyCoreInstructions { instructions })
    }

    /// Build core sell instructions (WSOL ATA, swap, close) — expensive, call once.
    pub fn build_core_sell_instructions(
        request: PumpAmmSellCoreRequest<'_>,
    ) -> Result<PumpAmmSellCoreInstructions, PumpAmmBuilderError> {
        ensure_supported_token_program(request.base_token_program)?;
        ensure_user_matches(request.payer, request.sell.accounts.user)?;

        let accounts = request.sell.accounts.clone();

        ensure_token_program_matches(request.base_token_program, accounts.base_token_program)?;
        ensure_quote_program_is_legacy(accounts.quote_token_program)?;

        let base_ata = derive_ata(
            &accounts.user,
            &accounts.base_mint,
            &request.base_token_program,
        );
        if base_ata != accounts.user_base_token_account {
            return Err(PumpAmmBuilderError::AssociatedBaseAccountMismatch {
                expected: accounts.user_base_token_account,
                derived: base_ata,
            });
        }

        let quote_ata = derive_ata(&accounts.user, &accounts.quote_mint, &LEGACY_TOKEN_PROGRAM);
        if quote_ata != accounts.user_quote_token_account {
            return Err(PumpAmmBuilderError::AssociatedQuoteAccountMismatch {
                expected: accounts.user_quote_token_account,
                derived: quote_ata,
            });
        }

        let sell_user = accounts.user;
        let mut instructions = Vec::with_capacity(3);

        // WSOL ATA creation
        instructions.push(create_wsol_ata_instruction(
            request.payer,
            accounts.user,
            accounts.quote_mint,
        ));

        // Swap instruction
        let sell_request = SellRequest {
            accounts: &accounts,
            base_amount_in: request.sell.base_amount_in,
            base_reserve: request.sell.base_reserve,
            quote_reserve: request.sell.quote_reserve,
            fees: request.sell.fees,
            slippage_bps: request.sell.slippage_bps,
        };
        instructions.push(PumpAmmTransactionBuilder::sell(sell_request)?);

        // Close WSOL ATA
        instructions.push(close_wsol_instruction(quote_ata, request.payer, sell_user)?);

        Ok(PumpAmmSellCoreInstructions { instructions })
    }

    /// Assemble full buy transaction: preamble + tip + core instructions.
    pub fn assemble_buy_with_preamble(
        preamble: &PumpAmmPreambleParams,
        core: &PumpAmmBuyCoreInstructions,
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
        preamble: &PumpAmmPreambleParams,
        core: &PumpAmmSellCoreInstructions,
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
        request: PumpAmmBuyTxRequest<'_>,
    ) -> Result<Vec<Instruction>, PumpAmmBuilderError> {
        let core_request = PumpAmmBuyCoreRequest {
            payer: request.common.payer,
            base_ata_owner: request.base_ata_owner,
            base_ata_payer: request.base_ata_payer,
            base_token_program: request.common.base_token_program,
            buy: request.buy,
        };
        let core = Self::build_core_buy_instructions(core_request)?;
        let preamble = PumpAmmPreambleParams {
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
        request: PumpAmmSellTxRequest<'_>,
    ) -> Result<Vec<Instruction>, PumpAmmBuilderError> {
        let core_request = PumpAmmSellCoreRequest {
            payer: request.common.payer,
            base_token_program: request.common.base_token_program,
            sell: request.sell,
        };
        let core = Self::build_core_sell_instructions(core_request)?;
        let preamble = PumpAmmPreambleParams {
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

fn buy_exact_quote_in_metas(accounts: &BuyExactQuoteInAccounts) -> Vec<AccountMeta> {
    let mut metas = Vec::with_capacity(23);
    metas.push(AccountMeta::new(accounts.pool, false));
    metas.push(AccountMeta::new(accounts.user, true));
    metas.push(AccountMeta::new_readonly(accounts.global_config, false));
    metas.push(AccountMeta::new_readonly(accounts.base_mint, false));
    metas.push(AccountMeta::new_readonly(accounts.quote_mint, false));
    metas.push(AccountMeta::new(accounts.user_base_token_account, false));
    metas.push(AccountMeta::new(accounts.user_quote_token_account, false));
    metas.push(AccountMeta::new(accounts.pool_base_token_account, false));
    metas.push(AccountMeta::new(accounts.pool_quote_token_account, false));
    metas.push(AccountMeta::new_readonly(
        accounts.protocol_fee_recipient,
        false,
    ));
    metas.push(AccountMeta::new(
        accounts.protocol_fee_recipient_token_account,
        false,
    ));
    metas.push(AccountMeta::new_readonly(
        accounts.base_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(
        accounts.quote_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(accounts.system_program, false));
    metas.push(AccountMeta::new_readonly(
        accounts.associated_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(accounts.event_authority, false));
    metas.push(AccountMeta::new_readonly(accounts.program, false));
    metas.push(AccountMeta::new(accounts.coin_creator_vault_ata, false));
    metas.push(AccountMeta::new_readonly(
        accounts.coin_creator_vault_authority,
        false,
    ));
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
    let mut metas = Vec::with_capacity(20);
    metas.push(AccountMeta::new(accounts.pool, false));
    metas.push(AccountMeta::new(accounts.user, true));
    metas.push(AccountMeta::new_readonly(accounts.global_config, false));
    metas.push(AccountMeta::new_readonly(accounts.base_mint, false));
    metas.push(AccountMeta::new_readonly(accounts.quote_mint, false));
    metas.push(AccountMeta::new(accounts.user_base_token_account, false));
    metas.push(AccountMeta::new(accounts.user_quote_token_account, false));
    metas.push(AccountMeta::new(accounts.pool_base_token_account, false));
    metas.push(AccountMeta::new(accounts.pool_quote_token_account, false));
    metas.push(AccountMeta::new_readonly(
        accounts.protocol_fee_recipient,
        false,
    ));
    metas.push(AccountMeta::new(
        accounts.protocol_fee_recipient_token_account,
        false,
    ));
    metas.push(AccountMeta::new_readonly(
        accounts.base_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(
        accounts.quote_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(accounts.system_program, false));
    metas.push(AccountMeta::new_readonly(
        accounts.associated_token_program,
        false,
    ));
    metas.push(AccountMeta::new_readonly(accounts.event_authority, false));
    metas.push(AccountMeta::new_readonly(accounts.program, false));
    metas.push(AccountMeta::new(accounts.coin_creator_vault_ata, false));
    metas.push(AccountMeta::new_readonly(
        accounts.coin_creator_vault_authority,
        false,
    ));
    metas.push(AccountMeta::new_readonly(accounts.fee_config, false));
    metas.push(AccountMeta::new_readonly(accounts.fee_program, false));
    metas
}

fn ensure_program_id(program: Pubkey) -> Result<(), PumpAmmBuilderError> {
    if program != PUMP_AMM_PROGRAM_ID {
        return Err(PumpAmmBuilderError::InvalidProgramId { provided: program });
    }
    Ok(())
}

fn apply_base_slippage(amount: u64, slippage_bps: u16) -> Option<u64> {
    if amount == 0 {
        return None;
    }
    let capped = slippage_bps.min(10_000);
    if capped == 10_000 {
        return None;
    }
    let numerator = (amount as u128) * (10_000u128 - capped as u128);
    let adjusted = (numerator / 10_000u128) as u64;
    if adjusted == 0 {
        None
    } else {
        Some(adjusted)
    }
}

pub fn user_volume_accumulator_pda(user: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"user_volume_accumulator", user.as_ref()],
        &PUMP_AMM_PROGRAM_ID,
    )
    .0
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
        base_mint: Pubkey,
        quote_mint: Pubkey,
        base_program: Pubkey,
    ) -> BuyExactQuoteInAccounts {
        let user_base_ata = derive_ata(&user, &base_mint, &base_program);
        let user_quote_ata = derive_ata(&user, &quote_mint, &LEGACY_TOKEN_PROGRAM);
        BuyExactQuoteInAccounts {
            pool: random_pubkey(),
            user,
            global_config: random_pubkey(),
            base_mint,
            quote_mint,
            user_base_token_account: user_base_ata,
            user_quote_token_account: user_quote_ata,
            pool_base_token_account: random_pubkey(),
            pool_quote_token_account: random_pubkey(),
            protocol_fee_recipient: random_pubkey(),
            protocol_fee_recipient_token_account: random_pubkey(),
            base_token_program: base_program,
            quote_token_program: LEGACY_TOKEN_PROGRAM,
            system_program: system_program::id(),
            associated_token_program: spl_associated_token_account::id(),
            event_authority: random_pubkey(),
            program: PUMP_AMM_PROGRAM_ID,
            coin_creator_vault_ata: random_pubkey(),
            coin_creator_vault_authority: random_pubkey(),
            global_volume_accumulator: random_pubkey(),
            user_volume_accumulator: random_pubkey(),
            fee_config: random_pubkey(),
            fee_program: random_pubkey(),
        }
    }

    fn sample_sell_accounts(
        user: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        base_program: Pubkey,
    ) -> SellAccounts {
        let user_base_ata = derive_ata(&user, &base_mint, &base_program);
        let user_quote_ata = derive_ata(&user, &quote_mint, &LEGACY_TOKEN_PROGRAM);
        SellAccounts {
            pool: random_pubkey(),
            user,
            global_config: random_pubkey(),
            base_mint,
            quote_mint,
            user_base_token_account: user_base_ata,
            user_quote_token_account: user_quote_ata,
            pool_base_token_account: random_pubkey(),
            pool_quote_token_account: random_pubkey(),
            protocol_fee_recipient: random_pubkey(),
            protocol_fee_recipient_token_account: random_pubkey(),
            base_token_program: base_program,
            quote_token_program: LEGACY_TOKEN_PROGRAM,
            system_program: system_program::id(),
            associated_token_program: spl_associated_token_account::id(),
            event_authority: random_pubkey(),
            program: PUMP_AMM_PROGRAM_ID,
            coin_creator_vault_ata: random_pubkey(),
            coin_creator_vault_authority: random_pubkey(),
            fee_config: random_pubkey(),
            fee_program: random_pubkey(),
        }
    }

    #[test]
    fn buy_transaction_includes_expected_instructions() {
        let user = random_pubkey();
        let base_mint = random_pubkey();
        let quote_mint = spl_token::native_mint::id();
        let base_program = LEGACY_TOKEN_PROGRAM;

        let accounts = sample_buy_accounts(user, base_mint, quote_mint, base_program);
        let buy_request = BuyExactQuoteInRequest {
            accounts: &accounts,
            spendable_quote_in: 1_000_000_000,
            base_reserve: 2_000_000_000,
            quote_reserve: 2_000_000_000,
            fees: Fees {
                lp_fee_bps: 50,
                protocol_fee_bps: 25,
                coin_creator_fee_bps: 0,
            },
            slippage_bps: 100,
            track_volume: false,
        };

        let common = PumpAmmCommonTxParams {
            payer: user,
            nonce_account: random_pubkey(),
            nonce_authority: user,
            compute_unit_price_micro_lamports: 1_000,
            tip_destination: random_pubkey(),
            tip_lamports: 10_000,
            cu_limit: DEFAULT_CU_LIMIT,
            base_token_program: base_program,
        };

        let request = PumpAmmBuyTxRequest {
            common,
            base_ata_owner: user,
            base_ata_payer: user,
            buy: buy_request,
        };

        let instructions = PumpAmmTxBuilder::build_buy_transaction(request).unwrap();
        assert_eq!(instructions.len(), 11);
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
        assert_eq!(
            instructions[6].program_id,
            spl_associated_token_account::id()
        );
        assert_eq!(instructions[7].program_id, system_program::id());
        assert_eq!(instructions[8].program_id, LEGACY_TOKEN_PROGRAM);
        assert_eq!(instructions[10].program_id, LEGACY_TOKEN_PROGRAM);

        let derived_uva = user_volume_accumulator_pda(&user);
        let pump_ix = &instructions[9];
        assert_eq!(
            pump_ix.accounts[pump_ix.accounts.len() - 3].pubkey,
            derived_uva
        );
        let args = BuyExactQuoteInArgs::try_from_slice(&pump_ix.data[8..]).expect("borsh decode");
        assert_eq!(args.spendable_quote_in, 1_000_000_000);
    }

    #[test]
    fn sell_transaction_includes_expected_instructions() {
        let user = random_pubkey();
        let base_mint = random_pubkey();
        let quote_mint = spl_token::native_mint::id();
        let base_program = TOKEN_2022_PROGRAM_ID;

        let accounts = sample_sell_accounts(user, base_mint, quote_mint, base_program);
        let sell_request = SellRequest {
            accounts: &accounts,
            base_amount_in: 1_000_000,
            base_reserve: 2_000_000_000,
            quote_reserve: 2_500_000_000,
            fees: Fees {
                lp_fee_bps: 50,
                protocol_fee_bps: 25,
                coin_creator_fee_bps: 10,
            },
            slippage_bps: 0,
        };

        let common = PumpAmmCommonTxParams {
            payer: user,
            nonce_account: random_pubkey(),
            nonce_authority: user,
            compute_unit_price_micro_lamports: 1_000,
            tip_destination: random_pubkey(),
            tip_lamports: 5_000,
            cu_limit: DEFAULT_CU_LIMIT,
            base_token_program: base_program,
        };

        let request = PumpAmmSellTxRequest {
            common,
            quote_wrap_amount: 500_000_000,
            sell: sell_request,
        };

        let instructions = PumpAmmTxBuilder::build_sell_transaction(request).unwrap();
        assert_eq!(instructions.len(), 8);
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
        assert_eq!(instructions[6].program_id, PUMP_AMM_PROGRAM_ID);
        assert_eq!(instructions[7].program_id, LEGACY_TOKEN_PROGRAM);
    }
}
