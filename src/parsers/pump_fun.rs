use super::{ParsedAction, ParserError, ParserResult, ProgramParser};
use super::{get_account_at_index, read_u64_le, read_i64_le, read_pubkey, read_bool};
use solana_sdk::{instruction::CompiledInstruction, pubkey, pubkey::Pubkey};

/// PumpFun program ID
pub const PUMP_FUN_PROGRAM_ID: Pubkey = pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");

/// PumpFun instruction discriminators (8 bytes, Anchor-style)
pub mod discriminators {
    pub const BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
    pub const BUY_EXACT_SOL_IN: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
    pub const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
}

/// PumpFun event discriminators (8 bytes for Anchor events)
pub mod event_discriminators {
    pub const TRADE_EVENT: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
}

/// Anchor event CPI discriminator - events emitted via self-CPI have this prefix
/// This is sha256("anchor:event")[..8]
const ANCHOR_EVENT_CPI_DISCRIMINATOR: [u8; 8] = [0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];

/// Container that bundles a PumpFun instruction with its associated event.
/// This is the "merge pattern" - instruction (intent) + event (outcome) together.
#[derive(Debug, Clone)]
pub struct PumpFunOperation {
    /// The parsed instruction (Buy, BuyExactSolIn, or Sell)
    pub instruction: PumpFunInstruction,
    /// The associated trade event, if emitted (None if tx failed before event)
    pub event: Option<PumpFunEvent>,
}

/// PumpFun instructions
#[derive(Debug, Clone, PartialEq)]
pub enum PumpFunInstruction {
    /// Buys tokens from a bonding curve.
    Buy {
        // args
        amount: u64,
        max_sol_cost: u64,
        track_volume: Option<bool>,
        // accounts (IDL order - 16 accounts)
        global: Pubkey,
        fee_recipient: Pubkey,
        mint: Pubkey,
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        associated_user: Pubkey,
        user: Pubkey,
        system_program: Pubkey,
        token_program: Pubkey,
        creator_vault: Pubkey,
        event_authority: Pubkey,
        program: Pubkey,
        global_volume_accumulator: Pubkey,
        user_volume_accumulator: Pubkey,
        fee_config: Pubkey,
        fee_program: Pubkey,
    },
    /// Given a budget of spendable SOL, buy at least min_tokens_out.
    /// Account creation and fees will be deducted from the spendable SOL.
    BuyExactSolIn {
        // args
        spendable_sol_in: u64,
        min_tokens_out: u64,
        track_volume: Option<bool>,
        // accounts (IDL order - same 16 accounts as Buy)
        global: Pubkey,
        fee_recipient: Pubkey,
        mint: Pubkey,
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        associated_user: Pubkey,
        user: Pubkey,
        system_program: Pubkey,
        token_program: Pubkey,
        creator_vault: Pubkey,
        event_authority: Pubkey,
        program: Pubkey,
        global_volume_accumulator: Pubkey,
        user_volume_accumulator: Pubkey,
        fee_config: Pubkey,
        fee_program: Pubkey,
    },
    /// Sells tokens into a bonding curve.
    Sell {
        // args
        amount: u64,
        min_sol_output: u64,
        // accounts (IDL order - 14 accounts, no volume accumulators)
        global: Pubkey,
        fee_recipient: Pubkey,
        mint: Pubkey,
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        associated_user: Pubkey,
        user: Pubkey,
        system_program: Pubkey,
        creator_vault: Pubkey,
        token_program: Pubkey,
        event_authority: Pubkey,
        program: Pubkey,
        fee_config: Pubkey,
        fee_program: Pubkey,
    },
}

/// PumpFun events
#[derive(Debug, Clone, PartialEq)]
pub enum PumpFunEvent {
    /// Trade event emitted on buy/sell/buy_exact_sol_in
    TradeEvent {
        mint: Pubkey,
        sol_amount: u64,
        token_amount: u64,
        is_buy: bool,
        user: Pubkey,
        timestamp: i64,
        virtual_sol_reserves: u64,
        virtual_token_reserves: u64,
        real_sol_reserves: u64,
        real_token_reserves: u64,
        fee_recipient: Pubkey,
        fee_basis_points: u64,
        fee: u64,
        creator: Pubkey,
        creator_fee_basis_points: u64,
        creator_fee: u64,
        track_volume: bool,
        total_unclaimed_tokens: u64,
        total_claimed_tokens: u64,
        current_sol_volume: u64,
        last_update_timestamp: i64,
        /// The instruction name that triggered this event: "buy" | "sell" | "buy_exact_sol_in"
        ix_name: String,
    },
}

/// PumpFun Parser implementation
#[derive(Debug)]
pub struct PumpFunParser;

impl PumpFunParser {
    pub fn new() -> Self { Self }

    /// Parse buy instruction arguments and accounts using zero-copy byte reading.
    /// Handles optional track_volume argument.
    /// 
    /// Layout after 8-byte discriminator:
    /// - amount: u64 (8 bytes) at offset 0
    /// - max_sol_cost: u64 (8 bytes) at offset 8
    /// - track_volume: OptionBool (1 byte) at offset 16 (optional)
    fn parse_buy(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> ParserResult<PumpFunInstruction> {
        let data = &instruction.data[8..]; // Skip discriminator
        
        // Zero-copy read of u64 args
        let amount = read_u64_le(data, 0)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read buy amount".to_string()))?;
        let max_sol_cost = read_u64_le(data, 8)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read buy max_sol_cost".to_string()))?;
        
        // Optional track_volume at offset 16
        let track_volume = read_bool(data, 16);

        Ok(PumpFunInstruction::Buy {
            amount,
            max_sol_cost,
            track_volume,
            global: get_account_at_index(instruction, account_keys, 0)?,
            fee_recipient: get_account_at_index(instruction, account_keys, 1)?,
            mint: get_account_at_index(instruction, account_keys, 2)?,
            bonding_curve: get_account_at_index(instruction, account_keys, 3)?,
            associated_bonding_curve: get_account_at_index(instruction, account_keys, 4)?,
            associated_user: get_account_at_index(instruction, account_keys, 5)?,
            user: get_account_at_index(instruction, account_keys, 6)?,
            system_program: get_account_at_index(instruction, account_keys, 7)?,
            token_program: get_account_at_index(instruction, account_keys, 8)?,
            creator_vault: get_account_at_index(instruction, account_keys, 9)?,
            event_authority: get_account_at_index(instruction, account_keys, 10)?,
            program: get_account_at_index(instruction, account_keys, 11)?,
            global_volume_accumulator: get_account_at_index(instruction, account_keys, 12)?,
            user_volume_accumulator: get_account_at_index(instruction, account_keys, 13)?,
            fee_config: get_account_at_index(instruction, account_keys, 14)?,
            fee_program: get_account_at_index(instruction, account_keys, 15)?,
        })
    }

    /// Parse buy_exact_sol_in instruction arguments and accounts using zero-copy byte reading.
    /// Handles optional track_volume argument.
    /// 
    /// Layout after 8-byte discriminator:
    /// - spendable_sol_in: u64 (8 bytes) at offset 0
    /// - min_tokens_out: u64 (8 bytes) at offset 8
    /// - track_volume: OptionBool (1 byte) at offset 16 (optional)
    fn parse_buy_exact_sol_in(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> ParserResult<PumpFunInstruction> {
        let data = &instruction.data[8..]; // Skip discriminator
        
        // Zero-copy read of u64 args
        let spendable_sol_in = read_u64_le(data, 0)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read spendable_sol_in".to_string()))?;
        let min_tokens_out = read_u64_le(data, 8)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read min_tokens_out".to_string()))?;
        
        // Optional track_volume at offset 16
        let track_volume = read_bool(data, 16);

        Ok(PumpFunInstruction::BuyExactSolIn {
            spendable_sol_in,
            min_tokens_out,
            track_volume,
            global: get_account_at_index(instruction, account_keys, 0)?,
            fee_recipient: get_account_at_index(instruction, account_keys, 1)?,
            mint: get_account_at_index(instruction, account_keys, 2)?,
            bonding_curve: get_account_at_index(instruction, account_keys, 3)?,
            associated_bonding_curve: get_account_at_index(instruction, account_keys, 4)?,
            associated_user: get_account_at_index(instruction, account_keys, 5)?,
            user: get_account_at_index(instruction, account_keys, 6)?,
            system_program: get_account_at_index(instruction, account_keys, 7)?,
            token_program: get_account_at_index(instruction, account_keys, 8)?,
            creator_vault: get_account_at_index(instruction, account_keys, 9)?,
            event_authority: get_account_at_index(instruction, account_keys, 10)?,
            program: get_account_at_index(instruction, account_keys, 11)?,
            global_volume_accumulator: get_account_at_index(instruction, account_keys, 12)?,
            user_volume_accumulator: get_account_at_index(instruction, account_keys, 13)?,
            fee_config: get_account_at_index(instruction, account_keys, 14)?,
            fee_program: get_account_at_index(instruction, account_keys, 15)?,
        })
    }

    /// Parse sell instruction arguments and accounts using zero-copy byte reading.
    /// Note: Sell has 14 accounts (no global_volume_accumulator, user_volume_accumulator)
    /// 
    /// Layout after 8-byte discriminator:
    /// - amount: u64 (8 bytes) at offset 0
    /// - min_sol_output: u64 (8 bytes) at offset 8
    fn parse_sell(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> ParserResult<PumpFunInstruction> {
        let data = &instruction.data[8..]; // Skip discriminator
        
        // Zero-copy read of u64 args
        let amount = read_u64_le(data, 0)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read sell amount".to_string()))?;
        let min_sol_output = read_u64_le(data, 8)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read sell min_sol_output".to_string()))?;

        Ok(PumpFunInstruction::Sell {
            amount,
            min_sol_output,
            global: get_account_at_index(instruction, account_keys, 0)?,
            fee_recipient: get_account_at_index(instruction, account_keys, 1)?,
            mint: get_account_at_index(instruction, account_keys, 2)?,
            bonding_curve: get_account_at_index(instruction, account_keys, 3)?,
            associated_bonding_curve: get_account_at_index(instruction, account_keys, 4)?,
            associated_user: get_account_at_index(instruction, account_keys, 5)?,
            user: get_account_at_index(instruction, account_keys, 6)?,
            system_program: get_account_at_index(instruction, account_keys, 7)?,
            creator_vault: get_account_at_index(instruction, account_keys, 8)?,
            token_program: get_account_at_index(instruction, account_keys, 9)?,
            event_authority: get_account_at_index(instruction, account_keys, 10)?,
            program: get_account_at_index(instruction, account_keys, 11)?,
            fee_config: get_account_at_index(instruction, account_keys, 12)?,
            fee_program: get_account_at_index(instruction, account_keys, 13)?,
        })
    }

    /// Try to parse event data, handling both:
    /// 1. Direct event data (discriminator at bytes 0-8)
    /// 2. Anchor CPI-wrapped events (CPI discriminator at 0-8, event discriminator at 8-16)
    fn try_parse_event_from_data(&self, data: &[u8]) -> Option<PumpFunEvent> {
        // Need at least 8 bytes for a discriminator
        if data.len() < 8 {
            return None;
        }
        
        let first_discriminator: [u8; 8] = data[..8].try_into().ok()?;
        
        // Check if this is a direct event (discriminator at start)
        if first_discriminator == event_discriminators::TRADE_EVENT {
            return self.parse_trade_event_data(&data[8..]);
        }
        
        // Check if this is an Anchor CPI-wrapped event
        // Format: [8 bytes CPI discriminator][8 bytes event discriminator][event data]
        if first_discriminator == ANCHOR_EVENT_CPI_DISCRIMINATOR && data.len() >= 16 {
            let event_discriminator: [u8; 8] = data[8..16].try_into().ok()?;
            
            if event_discriminator == event_discriminators::TRADE_EVENT {
                return self.parse_trade_event_data(&data[16..]);
            }
        }
        
        None
    }
    
    /// Parse the TradeEvent data using zero-copy byte reading.
    /// 
    /// Layout:
    /// - mint: Pubkey (32 bytes) at offset 0
    /// - sol_amount: u64 (8 bytes) at offset 32
    /// - token_amount: u64 (8 bytes) at offset 40
    /// - is_buy: bool (1 byte) at offset 48
    /// - user: Pubkey (32 bytes) at offset 49
    /// - timestamp: i64 (8 bytes) at offset 81
    /// - virtual_sol_reserves: u64 (8 bytes) at offset 89
    /// - virtual_token_reserves: u64 (8 bytes) at offset 97
    /// - real_sol_reserves: u64 (8 bytes) at offset 105
    /// - real_token_reserves: u64 (8 bytes) at offset 113
    /// - fee_recipient: Pubkey (32 bytes) at offset 121
    /// - fee_basis_points: u64 (8 bytes) at offset 153
    /// - fee: u64 (8 bytes) at offset 161
    /// - creator: Pubkey (32 bytes) at offset 169
    /// - creator_fee_basis_points: u64 (8 bytes) at offset 201
    /// - creator_fee: u64 (8 bytes) at offset 209
    /// - track_volume: bool (1 byte) at offset 217
    /// - total_unclaimed_tokens: u64 (8 bytes) at offset 218
    /// - total_claimed_tokens: u64 (8 bytes) at offset 226
    /// - current_sol_volume: u64 (8 bytes) at offset 234
    /// - last_update_timestamp: i64 (8 bytes) at offset 242
    /// - ix_name: String (4 bytes length + data) at offset 250
    fn parse_trade_event_data(&self, data: &[u8]) -> Option<PumpFunEvent> {
        // Minimum size check (250 bytes for fixed fields + 4 for string length)
        if data.len() < 254 {
            return None;
        }
        
        let mint = read_pubkey(data, 0)?;
        let sol_amount = read_u64_le(data, 32)?;
        let token_amount = read_u64_le(data, 40)?;
        let is_buy = read_bool(data, 48)?;
        let user = read_pubkey(data, 49)?;
        let timestamp = read_i64_le(data, 81)?;
        let virtual_sol_reserves = read_u64_le(data, 89)?;
        let virtual_token_reserves = read_u64_le(data, 97)?;
        let real_sol_reserves = read_u64_le(data, 105)?;
        let real_token_reserves = read_u64_le(data, 113)?;
        let fee_recipient = read_pubkey(data, 121)?;
        let fee_basis_points = read_u64_le(data, 153)?;
        let fee = read_u64_le(data, 161)?;
        let creator = read_pubkey(data, 169)?;
        let creator_fee_basis_points = read_u64_le(data, 201)?;
        let creator_fee = read_u64_le(data, 209)?;
        let track_volume = read_bool(data, 217)?;
        let total_unclaimed_tokens = read_u64_le(data, 218)?;
        let total_claimed_tokens = read_u64_le(data, 226)?;
        let current_sol_volume = read_u64_le(data, 234)?;
        let last_update_timestamp = read_i64_le(data, 242)?;
        
        // Parse Borsh string: 4-byte length prefix + UTF-8 data
        let ix_name = read_borsh_string(data, 250)?;
        
        Some(PumpFunEvent::TradeEvent {
            mint,
            sol_amount,
            token_amount,
            is_buy,
            user,
            timestamp,
            virtual_sol_reserves,
            virtual_token_reserves,
            real_sol_reserves,
            real_token_reserves,
            fee_recipient,
            fee_basis_points,
            fee,
            creator,
            creator_fee_basis_points,
            creator_fee,
            track_volume,
            total_unclaimed_tokens,
            total_claimed_tokens,
            current_sol_volume,
            last_update_timestamp,
            ix_name,
        })
    }

    /// Scan inner instructions to find the associated TradeEvent.
    /// Uses linear look-ahead: the event will appear after the instruction in the flattened list.
    fn find_event_in_inner_instructions(
        &self,
        inner_instructions: &[CompiledInstruction],
        account_keys: &[Pubkey],
    ) -> Option<PumpFunEvent> {
        for inner_ix in inner_instructions {
            // Check if this inner instruction is from PumpFun program
            let inner_program_id = account_keys.get(inner_ix.program_id_index as usize);
            
            if inner_program_id == Some(&PUMP_FUN_PROGRAM_ID) {
                // Try to parse as event
                if let Some(event) = self.try_parse_event_from_data(&inner_ix.data) {
                    return Some(event);
                }
            }
        }
        None
    }
}

/// Read a Borsh-encoded string (4-byte little-endian length prefix + UTF-8 data)
#[inline]
fn read_borsh_string(data: &[u8], offset: usize) -> Option<String> {
    if data.len() < offset + 4 {
        return None;
    }
    
    // Read 4-byte length prefix
    let len_bytes: [u8; 4] = data[offset..offset + 4].try_into().ok()?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    
    // Check we have enough data for the string
    if data.len() < offset + 4 + len {
        return None;
    }
    
    // Parse UTF-8 string
    let str_bytes = &data[offset + 4..offset + 4 + len];
    String::from_utf8(str_bytes.to_vec()).ok()
}

impl ProgramParser for PumpFunParser {
    fn parse_instruction_and_merge(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
        inner_instructions: &[CompiledInstruction],
    ) -> ParserResult<Option<ParsedAction>> {
        // 1. Check if we have enough data for an Anchor discriminator
        if instruction.data.len() < 8 {
            return Ok(None);
        }
        
        let discriminator: [u8; 8] = instruction.data[..8].try_into()
            .map_err(|_| ParserError::InvalidInstruction("Failed to read discriminator".to_string()))?;
        
        // 2. Try to parse as instruction (Buy, BuyExactSolIn, or Sell)
        let ix_data = match discriminator {
            discriminators::BUY => self.parse_buy(instruction, account_keys)?,
            discriminators::BUY_EXACT_SOL_IN => self.parse_buy_exact_sol_in(instruction, account_keys)?,
            discriminators::SELL => self.parse_sell(instruction, account_keys)?,
            // If discriminator doesn't match known instructions, this isn't something we handle
            // (could be event data or unknown instruction)
            _ => return Ok(None),
        };
        
        // 3. Look for the associated event in inner instructions (look-ahead)
        let found_event = self.find_event_in_inner_instructions(inner_instructions, account_keys);
        
        // 4. Return the bundled operation
        Ok(Some(ParsedAction::PumpFun(PumpFunOperation {
            instruction: ix_data,
            event: found_event,
        })))
    }

    fn program_id(&self) -> Pubkey { 
        PUMP_FUN_PROGRAM_ID 
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pump_fun_parser_creation() {
        let parser = PumpFunParser::new();
        assert_eq!(parser.program_id(), PUMP_FUN_PROGRAM_ID);
    }
    
    #[test]
    fn test_discriminator_values() {
        // Verify discriminator constants are correct
        assert_eq!(discriminators::BUY.len(), 8);
        assert_eq!(discriminators::BUY_EXACT_SOL_IN.len(), 8);
        assert_eq!(discriminators::SELL.len(), 8);
        assert_eq!(event_discriminators::TRADE_EVENT.len(), 8);
    }
    
    #[test]
    fn test_read_borsh_string() {
        // Test with "buy" string
        let mut data = vec![0u8; 10];
        // Length prefix (3 as u32 little-endian)
        data[0..4].copy_from_slice(&3u32.to_le_bytes());
        // String data "buy"
        data[4..7].copy_from_slice(b"buy");
        
        let result = read_borsh_string(&data, 0);
        assert_eq!(result, Some("buy".to_string()));
    }
}
