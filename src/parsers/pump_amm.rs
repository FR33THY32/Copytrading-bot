use super::{ParsedAction, ParserError, ParserResult, ProgramParser};
use super::{get_account_at_index, read_u64_le, read_i64_le, read_pubkey, read_bool};
use solana_sdk::{instruction::CompiledInstruction, pubkey, pubkey::Pubkey};

/// Pump AMM program ID
pub const PUMP_AMM_PROGRAM_ID: Pubkey = pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");

/// Pump AMM instruction discriminators (8 bytes, Anchor-style)
pub mod discriminators {
    pub const BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
    pub const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
}

/// Pump AMM event discriminators (8 bytes, after Anchor discriminator)
pub mod event_discriminators {
    pub const BUY_EVENT: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
    pub const SELL_EVENT: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];
}

/// Anchor event CPI discriminator - events emitted via self-CPI have this prefix
/// This is sha256("anchor:event")[..8]
const ANCHOR_EVENT_CPI_DISCRIMINATOR: [u8; 8] = [0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];

/// Container that bundles a Pump AMM instruction with its associated event.
/// This is the "merge pattern" - instruction (intent) + event (outcome) together.
#[derive(Debug, Clone)]
pub struct PumpAmmOperation {
    /// The parsed instruction (Buy or Sell)
    pub instruction: PumpAmmInstruction,
    /// The associated event, if emitted (None if tx failed before event)
    pub event: Option<PumpAmmEvent>,
}

/// Pump AMM instructions
#[derive(Debug, Clone, PartialEq)]
pub enum PumpAmmInstruction {
    Buy {
        // args
        base_amount_out: u64,
        max_quote_amount_in: u64,
        track_volume: Option<bool>,
        // accounts (IDL order)
        pool: Pubkey,
        user: Pubkey,
        global_config: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        user_base_token_account: Pubkey,
        user_quote_token_account: Pubkey,
        pool_base_token_account: Pubkey,
        pool_quote_token_account: Pubkey,
        protocol_fee_recipient: Pubkey,
        protocol_fee_recipient_token_account: Pubkey,
        base_token_program: Pubkey,
        quote_token_program: Pubkey,
        system_program: Pubkey,
        associated_token_program: Pubkey,
        event_authority: Pubkey,
        program: Pubkey,
        coin_creator_vault_ata: Pubkey,
        coin_creator_vault_authority: Pubkey,
        global_volume_accumulator: Pubkey,
        user_volume_accumulator: Pubkey,
        fee_config: Pubkey,
        fee_program: Pubkey,
    },
    Sell {
        // args
        base_amount_in: u64,
        min_quote_amount_out: u64,
        // accounts (IDL order)
        pool: Pubkey,
        user: Pubkey,
        global_config: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        user_base_token_account: Pubkey,
        user_quote_token_account: Pubkey,
        pool_base_token_account: Pubkey,
        pool_quote_token_account: Pubkey,
        protocol_fee_recipient: Pubkey,
        protocol_fee_recipient_token_account: Pubkey,
        base_token_program: Pubkey,
        quote_token_program: Pubkey,
        system_program: Pubkey,
        associated_token_program: Pubkey,
        event_authority: Pubkey,
        program: Pubkey,
        coin_creator_vault_ata: Pubkey,
        coin_creator_vault_authority: Pubkey,
        fee_config: Pubkey,
        fee_program: Pubkey,
    },
}

/// Pump AMM events
#[derive(Debug, Clone, PartialEq)]
pub enum PumpAmmEvent {
    BuyEvent {
        timestamp: i64,
        base_amount_out: u64,
        max_quote_amount_in: u64,
        user_base_token_reserves: u64,
        user_quote_token_reserves: u64,
        pool_base_token_reserves: u64,
        pool_quote_token_reserves: u64,
        quote_amount_in: u64,
        lp_fee_basis_points: u64,
        lp_fee: u64,
        protocol_fee_basis_points: u64,
        protocol_fee: u64,
        quote_amount_in_with_lp_fee: u64,
        user_quote_amount_in: u64,
        pool: Pubkey,
        user: Pubkey,
        user_base_token_account: Pubkey,
        user_quote_token_account: Pubkey,
        protocol_fee_recipient: Pubkey,
        protocol_fee_recipient_token_account: Pubkey,
        coin_creator: Pubkey,
        coin_creator_fee_basis_points: u64,
        coin_creator_fee: u64,
        track_volume: bool,
        total_unclaimed_tokens: u64,
        total_claimed_tokens: u64,
        current_sol_volume: u64,
        last_update_timestamp: i64,
    },
    SellEvent {
        timestamp: i64,
        base_amount_in: u64,
        min_quote_amount_out: u64,
        user_base_token_reserves: u64,
        user_quote_token_reserves: u64,
        pool_base_token_reserves: u64,
        pool_quote_token_reserves: u64,
        quote_amount_out: u64,
        lp_fee_basis_points: u64,
        lp_fee: u64,
        protocol_fee_basis_points: u64,
        protocol_fee: u64,
        quote_amount_out_without_lp_fee: u64,
        user_quote_amount_out: u64,
        pool: Pubkey,
        user: Pubkey,
        user_base_token_account: Pubkey,
        user_quote_token_account: Pubkey,
        protocol_fee_recipient: Pubkey,
        protocol_fee_recipient_token_account: Pubkey,
        coin_creator: Pubkey,
        coin_creator_fee_basis_points: u64,
        coin_creator_fee: u64,
    },
}

/// Pump AMM Parser implementation
#[derive(Debug)]
pub struct PumpAmmParser;

impl PumpAmmParser {
    pub fn new() -> Self { Self }

    /// Parse buy instruction arguments and accounts using zero-copy byte reading.
    /// 
    /// Layout after 8-byte discriminator:
    /// - base_amount_out: u64 (8 bytes) at offset 0
    /// - max_quote_amount_in: u64 (8 bytes) at offset 8
    /// - track_volume: Option<bool> (1 byte discriminant + optional 1 byte) at offset 16
    fn parse_buy(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> ParserResult<PumpAmmInstruction> {
        let data = &instruction.data[8..]; // Skip discriminator
        
        // Zero-copy read of u64 args
        let base_amount_out = read_u64_le(data, 0)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read base_amount_out".to_string()))?;
        let max_quote_amount_in = read_u64_le(data, 8)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read max_quote_amount_in".to_string()))?;
        
        // Parse Option<bool> - Borsh encodes as: 0 = None, 1 = Some(value)
        let track_volume = read_option_bool(data, 16);

        Ok(PumpAmmInstruction::Buy {
            base_amount_out,
            max_quote_amount_in,
            track_volume,
            pool: get_account_at_index(instruction, account_keys, 0)?,
            user: get_account_at_index(instruction, account_keys, 1)?,
            global_config: get_account_at_index(instruction, account_keys, 2)?,
            base_mint: get_account_at_index(instruction, account_keys, 3)?,
            quote_mint: get_account_at_index(instruction, account_keys, 4)?,
            user_base_token_account: get_account_at_index(instruction, account_keys, 5)?,
            user_quote_token_account: get_account_at_index(instruction, account_keys, 6)?,
            pool_base_token_account: get_account_at_index(instruction, account_keys, 7)?,
            pool_quote_token_account: get_account_at_index(instruction, account_keys, 8)?,
            protocol_fee_recipient: get_account_at_index(instruction, account_keys, 9)?,
            protocol_fee_recipient_token_account: get_account_at_index(instruction, account_keys, 10)?,
            base_token_program: get_account_at_index(instruction, account_keys, 11)?,
            quote_token_program: get_account_at_index(instruction, account_keys, 12)?,
            system_program: get_account_at_index(instruction, account_keys, 13)?,
            associated_token_program: get_account_at_index(instruction, account_keys, 14)?,
            event_authority: get_account_at_index(instruction, account_keys, 15)?,
            program: get_account_at_index(instruction, account_keys, 16)?,
            coin_creator_vault_ata: get_account_at_index(instruction, account_keys, 17)?,
            coin_creator_vault_authority: get_account_at_index(instruction, account_keys, 18)?,
            global_volume_accumulator: get_account_at_index(instruction, account_keys, 19)?,
            user_volume_accumulator: get_account_at_index(instruction, account_keys, 20)?,
            fee_config: get_account_at_index(instruction, account_keys, 21)?,
            fee_program: get_account_at_index(instruction, account_keys, 22)?,
        })
    }

    /// Parse sell instruction arguments and accounts using zero-copy byte reading.
    /// 
    /// Layout after 8-byte discriminator:
    /// - base_amount_in: u64 (8 bytes) at offset 0
    /// - min_quote_amount_out: u64 (8 bytes) at offset 8
    fn parse_sell(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> ParserResult<PumpAmmInstruction> {
        let data = &instruction.data[8..]; // Skip discriminator
        
        // Zero-copy read of u64 args
        let base_amount_in = read_u64_le(data, 0)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read base_amount_in".to_string()))?;
        let min_quote_amount_out = read_u64_le(data, 8)
            .ok_or_else(|| ParserError::DeserializationError("Failed to read min_quote_amount_out".to_string()))?;

        Ok(PumpAmmInstruction::Sell {
            base_amount_in,
            min_quote_amount_out,
            pool: get_account_at_index(instruction, account_keys, 0)?,
            user: get_account_at_index(instruction, account_keys, 1)?,
            global_config: get_account_at_index(instruction, account_keys, 2)?,
            base_mint: get_account_at_index(instruction, account_keys, 3)?,
            quote_mint: get_account_at_index(instruction, account_keys, 4)?,
            user_base_token_account: get_account_at_index(instruction, account_keys, 5)?,
            user_quote_token_account: get_account_at_index(instruction, account_keys, 6)?,
            pool_base_token_account: get_account_at_index(instruction, account_keys, 7)?,
            pool_quote_token_account: get_account_at_index(instruction, account_keys, 8)?,
            protocol_fee_recipient: get_account_at_index(instruction, account_keys, 9)?,
            protocol_fee_recipient_token_account: get_account_at_index(instruction, account_keys, 10)?,
            base_token_program: get_account_at_index(instruction, account_keys, 11)?,
            quote_token_program: get_account_at_index(instruction, account_keys, 12)?,
            system_program: get_account_at_index(instruction, account_keys, 13)?,
            associated_token_program: get_account_at_index(instruction, account_keys, 14)?,
            event_authority: get_account_at_index(instruction, account_keys, 15)?,
            program: get_account_at_index(instruction, account_keys, 16)?,
            coin_creator_vault_ata: get_account_at_index(instruction, account_keys, 17)?,
            coin_creator_vault_authority: get_account_at_index(instruction, account_keys, 18)?,
            fee_config: get_account_at_index(instruction, account_keys, 19)?,
            fee_program: get_account_at_index(instruction, account_keys, 20)?,
        })
    }

    /// Try to parse a BuyEvent from raw instruction data.
    /// Handles both:
    /// 1. Direct event data (discriminator at bytes 0-8)
    /// 2. Anchor CPI-wrapped events (CPI discriminator at 0-8, event discriminator at 8-16)
    fn try_parse_buy_event(&self, data: &[u8]) -> Option<PumpAmmEvent> {
        if data.len() < 8 {
            return None;
        }
        
        let first_discriminator: [u8; 8] = data[..8].try_into().ok()?;
        
        // Check if this is a direct event (discriminator at start)
        if first_discriminator == event_discriminators::BUY_EVENT {
            return self.parse_buy_event_data(&data[8..]);
        }
        
        // Check if this is an Anchor CPI-wrapped event
        if first_discriminator == ANCHOR_EVENT_CPI_DISCRIMINATOR && data.len() >= 16 {
            let event_discriminator: [u8; 8] = data[8..16].try_into().ok()?;
            
            if event_discriminator == event_discriminators::BUY_EVENT {
                return self.parse_buy_event_data(&data[16..]);
            }
        }
        
        None
    }
    
    /// Parse BuyEvent data using zero-copy byte reading.
    /// 
    /// Layout:
    /// - timestamp: i64 (8 bytes) at offset 0
    /// - base_amount_out: u64 (8 bytes) at offset 8
    /// - max_quote_amount_in: u64 (8 bytes) at offset 16
    /// - user_base_token_reserves: u64 (8 bytes) at offset 24
    /// - user_quote_token_reserves: u64 (8 bytes) at offset 32
    /// - pool_base_token_reserves: u64 (8 bytes) at offset 40
    /// - pool_quote_token_reserves: u64 (8 bytes) at offset 48
    /// - quote_amount_in: u64 (8 bytes) at offset 56
    /// - lp_fee_basis_points: u64 (8 bytes) at offset 64
    /// - lp_fee: u64 (8 bytes) at offset 72
    /// - protocol_fee_basis_points: u64 (8 bytes) at offset 80
    /// - protocol_fee: u64 (8 bytes) at offset 88
    /// - quote_amount_in_with_lp_fee: u64 (8 bytes) at offset 96
    /// - user_quote_amount_in: u64 (8 bytes) at offset 104
    /// - pool: Pubkey (32 bytes) at offset 112
    /// - user: Pubkey (32 bytes) at offset 144
    /// - user_base_token_account: Pubkey (32 bytes) at offset 176
    /// - user_quote_token_account: Pubkey (32 bytes) at offset 208
    /// - protocol_fee_recipient: Pubkey (32 bytes) at offset 240
    /// - protocol_fee_recipient_token_account: Pubkey (32 bytes) at offset 272
    /// - coin_creator: Pubkey (32 bytes) at offset 304
    /// - coin_creator_fee_basis_points: u64 (8 bytes) at offset 336
    /// - coin_creator_fee: u64 (8 bytes) at offset 344
    /// - track_volume: bool (1 byte) at offset 352
    /// - total_unclaimed_tokens: u64 (8 bytes) at offset 353
    /// - total_claimed_tokens: u64 (8 bytes) at offset 361
    /// - current_sol_volume: u64 (8 bytes) at offset 369
    /// - last_update_timestamp: i64 (8 bytes) at offset 377
    fn parse_buy_event_data(&self, data: &[u8]) -> Option<PumpAmmEvent> {
        // Minimum size check
        if data.len() < 385 {
            return None;
        }
        
        Some(PumpAmmEvent::BuyEvent {
            timestamp: read_i64_le(data, 0)?,
            base_amount_out: read_u64_le(data, 8)?,
            max_quote_amount_in: read_u64_le(data, 16)?,
            user_base_token_reserves: read_u64_le(data, 24)?,
            user_quote_token_reserves: read_u64_le(data, 32)?,
            pool_base_token_reserves: read_u64_le(data, 40)?,
            pool_quote_token_reserves: read_u64_le(data, 48)?,
            quote_amount_in: read_u64_le(data, 56)?,
            lp_fee_basis_points: read_u64_le(data, 64)?,
            lp_fee: read_u64_le(data, 72)?,
            protocol_fee_basis_points: read_u64_le(data, 80)?,
            protocol_fee: read_u64_le(data, 88)?,
            quote_amount_in_with_lp_fee: read_u64_le(data, 96)?,
            user_quote_amount_in: read_u64_le(data, 104)?,
            pool: read_pubkey(data, 112)?,
            user: read_pubkey(data, 144)?,
            user_base_token_account: read_pubkey(data, 176)?,
            user_quote_token_account: read_pubkey(data, 208)?,
            protocol_fee_recipient: read_pubkey(data, 240)?,
            protocol_fee_recipient_token_account: read_pubkey(data, 272)?,
            coin_creator: read_pubkey(data, 304)?,
            coin_creator_fee_basis_points: read_u64_le(data, 336)?,
            coin_creator_fee: read_u64_le(data, 344)?,
            track_volume: read_bool(data, 352)?,
            total_unclaimed_tokens: read_u64_le(data, 353)?,
            total_claimed_tokens: read_u64_le(data, 361)?,
            current_sol_volume: read_u64_le(data, 369)?,
            last_update_timestamp: read_i64_le(data, 377)?,
        })
    }

    /// Try to parse a SellEvent from raw instruction data.
    /// Handles both:
    /// 1. Direct event data (discriminator at bytes 0-8)
    /// 2. Anchor CPI-wrapped events (CPI discriminator at 0-8, event discriminator at 8-16)
    fn try_parse_sell_event(&self, data: &[u8]) -> Option<PumpAmmEvent> {
        if data.len() < 8 {
            return None;
        }
        
        let first_discriminator: [u8; 8] = data[..8].try_into().ok()?;
        
        // Check if this is a direct event (discriminator at start)
        if first_discriminator == event_discriminators::SELL_EVENT {
            return self.parse_sell_event_data(&data[8..]);
        }
        
        // Check if this is an Anchor CPI-wrapped event
        if first_discriminator == ANCHOR_EVENT_CPI_DISCRIMINATOR && data.len() >= 16 {
            let event_discriminator: [u8; 8] = data[8..16].try_into().ok()?;
            
            if event_discriminator == event_discriminators::SELL_EVENT {
                return self.parse_sell_event_data(&data[16..]);
            }
        }
        
        None
    }
    
    /// Parse SellEvent data using zero-copy byte reading.
    /// 
    /// Layout:
    /// - timestamp: i64 (8 bytes) at offset 0
    /// - base_amount_in: u64 (8 bytes) at offset 8
    /// - min_quote_amount_out: u64 (8 bytes) at offset 16
    /// - user_base_token_reserves: u64 (8 bytes) at offset 24
    /// - user_quote_token_reserves: u64 (8 bytes) at offset 32
    /// - pool_base_token_reserves: u64 (8 bytes) at offset 40
    /// - pool_quote_token_reserves: u64 (8 bytes) at offset 48
    /// - quote_amount_out: u64 (8 bytes) at offset 56
    /// - lp_fee_basis_points: u64 (8 bytes) at offset 64
    /// - lp_fee: u64 (8 bytes) at offset 72
    /// - protocol_fee_basis_points: u64 (8 bytes) at offset 80
    /// - protocol_fee: u64 (8 bytes) at offset 88
    /// - quote_amount_out_without_lp_fee: u64 (8 bytes) at offset 96
    /// - user_quote_amount_out: u64 (8 bytes) at offset 104
    /// - pool: Pubkey (32 bytes) at offset 112
    /// - user: Pubkey (32 bytes) at offset 144
    /// - user_base_token_account: Pubkey (32 bytes) at offset 176
    /// - user_quote_token_account: Pubkey (32 bytes) at offset 208
    /// - protocol_fee_recipient: Pubkey (32 bytes) at offset 240
    /// - protocol_fee_recipient_token_account: Pubkey (32 bytes) at offset 272
    /// - coin_creator: Pubkey (32 bytes) at offset 304
    /// - coin_creator_fee_basis_points: u64 (8 bytes) at offset 336
    /// - coin_creator_fee: u64 (8 bytes) at offset 344
    fn parse_sell_event_data(&self, data: &[u8]) -> Option<PumpAmmEvent> {
        // Minimum size check
        if data.len() < 352 {
            return None;
        }
        
        Some(PumpAmmEvent::SellEvent {
            timestamp: read_i64_le(data, 0)?,
            base_amount_in: read_u64_le(data, 8)?,
            min_quote_amount_out: read_u64_le(data, 16)?,
            user_base_token_reserves: read_u64_le(data, 24)?,
            user_quote_token_reserves: read_u64_le(data, 32)?,
            pool_base_token_reserves: read_u64_le(data, 40)?,
            pool_quote_token_reserves: read_u64_le(data, 48)?,
            quote_amount_out: read_u64_le(data, 56)?,
            lp_fee_basis_points: read_u64_le(data, 64)?,
            lp_fee: read_u64_le(data, 72)?,
            protocol_fee_basis_points: read_u64_le(data, 80)?,
            protocol_fee: read_u64_le(data, 88)?,
            quote_amount_out_without_lp_fee: read_u64_le(data, 96)?,
            user_quote_amount_out: read_u64_le(data, 104)?,
            pool: read_pubkey(data, 112)?,
            user: read_pubkey(data, 144)?,
            user_base_token_account: read_pubkey(data, 176)?,
            user_quote_token_account: read_pubkey(data, 208)?,
            protocol_fee_recipient: read_pubkey(data, 240)?,
            protocol_fee_recipient_token_account: read_pubkey(data, 272)?,
            coin_creator: read_pubkey(data, 304)?,
            coin_creator_fee_basis_points: read_u64_le(data, 336)?,
            coin_creator_fee: read_u64_le(data, 344)?,
        })
    }

    /// Scan inner instructions to find the associated event (Buy or Sell).
    /// Uses linear look-ahead: the event will appear after the instruction in the flattened list.
    fn find_event_in_inner_instructions(
        &self,
        inner_instructions: &[CompiledInstruction],
        account_keys: &[Pubkey],
        is_buy: bool,
    ) -> Option<PumpAmmEvent> {
        for inner_ix in inner_instructions {
            // Check if this inner instruction is from Pump AMM program
            let inner_program_id = account_keys.get(inner_ix.program_id_index as usize);
            
            if inner_program_id == Some(&PUMP_AMM_PROGRAM_ID) {
                // Try to parse as the appropriate event type
                let event = if is_buy {
                    self.try_parse_buy_event(&inner_ix.data)
                } else {
                    self.try_parse_sell_event(&inner_ix.data)
                };
                
                if event.is_some() {
                    return event;
                }
            }
        }
        None
    }
}

/// Read a Borsh-encoded Option<bool> (0 = None, 1 = Some(value))
#[inline]
fn read_option_bool(data: &[u8], offset: usize) -> Option<bool> {
    if data.len() <= offset {
        return None;
    }
    
    match data[offset] {
        0 => None, // None variant
        1 => {
            // Some variant - read the bool value
            if data.len() > offset + 1 {
                Some(data[offset + 1] != 0)
            } else {
                None
            }
        }
        _ => None, // Invalid discriminant
    }
}

impl ProgramParser for PumpAmmParser {
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
        
        // 2. Try to parse as instruction (Buy or Sell)
        let (ix_data, is_buy) = match discriminator {
            discriminators::BUY => (self.parse_buy(instruction, account_keys)?, true),
            discriminators::SELL => (self.parse_sell(instruction, account_keys)?, false),
            // If discriminator doesn't match Buy/Sell, this isn't an instruction we handle
            _ => return Ok(None),
        };
        
        // 3. Look for the associated event in inner instructions (look-ahead)
        let found_event = self.find_event_in_inner_instructions(inner_instructions, account_keys, is_buy);
        
        // 4. Return the bundled operation
        Ok(Some(ParsedAction::PumpAmm(PumpAmmOperation {
            instruction: ix_data,
            event: found_event,
        })))
    }

    fn program_id(&self) -> Pubkey { 
        PUMP_AMM_PROGRAM_ID 
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pump_amm_parser_creation() {
        let parser = PumpAmmParser::new();
        assert_eq!(parser.program_id(), PUMP_AMM_PROGRAM_ID);
    }
    
    #[test]
    fn test_discriminator_values() {
        // Verify discriminator constants are correct
        assert_eq!(discriminators::BUY.len(), 8);
        assert_eq!(discriminators::SELL.len(), 8);
        assert_eq!(event_discriminators::BUY_EVENT.len(), 8);
        assert_eq!(event_discriminators::SELL_EVENT.len(), 8);
    }
    
    #[test]
    fn test_read_option_bool() {
        // Test None (0)
        let data_none = [0u8];
        assert_eq!(read_option_bool(&data_none, 0), None);
        
        // Test Some(false)
        let data_some_false = [1u8, 0u8];
        assert_eq!(read_option_bool(&data_some_false, 0), Some(false));
        
        // Test Some(true)
        let data_some_true = [1u8, 1u8];
        assert_eq!(read_option_bool(&data_some_true, 0), Some(true));
    }
}
