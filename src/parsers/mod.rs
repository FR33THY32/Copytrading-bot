use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use std::fmt::Debug;
use thiserror::Error;

pub mod pump_fun;
pub mod pump_amm;

/// Result type for parser operations
pub type ParserResult<T> = Result<T, ParserError>;

/// Error types for parsing operations
#[derive(Debug, Error)]
pub enum ParserError {
    #[error("Invalid instruction data: {0}")]
    InvalidInstruction(String),
    
    #[error("Invalid event data: {0}")]
    InvalidEvent(String),
    
    #[error("Account index out of bounds: {index}")]
    AccountIndexOutOfBounds { index: usize },
    
    #[error("Missing required account: {0}")]
    MissingAccount(String),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Unknown instruction discriminator: {0}")]
    UnknownDiscriminator(u8),
    
    #[error("Parser not implemented for program: {0}")]
    ParserNotImplemented(Pubkey),
    
    #[error("Other error: {0}")]
    Other(String),
}

/// Parsed action representation - bundles instruction with its associated event
/// This is the "merge pattern" output: instruction (intent) + event (outcome) together
#[derive(Debug, Clone)]
pub enum ParsedAction {
    /// PumpFun program operation (instruction + event paired)
    PumpFun(pump_fun::PumpFunOperation),
    /// Pump AMM program operation (instruction + event paired)
    PumpAmm(pump_amm::PumpAmmOperation),
    
    // Add more program operation types here as needed:
    // Raydium(raydium::RaydiumOperation),
    // Jupiter(jupiter::JupiterOperation),
}

/// Trait that all program parsers must implement
/// 
/// The merge pattern: parse an instruction and immediately look for its
/// associated event in the inner instructions, returning them bundled together.
pub trait ProgramParser: Send + Sync + Debug {
    /// Parse an instruction and merge with its associated event from inner instructions.
    /// 
    /// # Arguments
    /// * `instruction` - The instruction to parse (could be outer or inner)
    /// * `account_keys` - All account keys from the transaction
    /// * `inner_instructions` - The remaining inner instructions to search for events
    ///   (for outer instructions: all inner ixs; for inner instructions: remaining slice)
    /// 
    /// # Returns
    /// * `Ok(Some(action))` - Successfully parsed instruction with optional event
    /// * `Ok(None)` - Instruction doesn't match this parser (wrong discriminator, event data, etc.)
    /// * `Err(e)` - Parsing error
    fn parse_instruction_and_merge(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
        inner_instructions: &[CompiledInstruction],
    ) -> ParserResult<Option<ParsedAction>>;
    
    /// Get the program ID this parser handles
    fn program_id(&self) -> Pubkey;
}

/// Helper function to safely get an account from instruction
pub fn get_account_at_index(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    index: usize,
) -> ParserResult<Pubkey> {
    instruction.accounts
        .get(index)
        .and_then(|&account_index| account_keys.get(account_index as usize))
        .copied()
        .ok_or_else(|| ParserError::AccountIndexOutOfBounds { index })
}

/// Helper function to get multiple accounts
pub fn get_accounts(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    indices: &[usize],
) -> ParserResult<Vec<Pubkey>> {
    indices
        .iter()
        .map(|&index| get_account_at_index(instruction, account_keys, index))
        .collect()
}

/// Helper to extract instruction discriminator (first byte)
pub fn get_discriminator(data: &[u8]) -> ParserResult<u8> {
    data.first()
        .copied()
        .ok_or_else(|| ParserError::InvalidInstruction("Empty instruction data".to_string()))
}

/// Helper to extract 8-byte discriminator (Anchor-style)
pub fn get_anchor_discriminator(data: &[u8]) -> ParserResult<[u8; 8]> {
    if data.len() < 8 {
        return Err(ParserError::InvalidInstruction(
            "Instruction data too short for anchor discriminator".to_string()
        ));
    }
    
    let mut discriminator = [0u8; 8];
    discriminator.copy_from_slice(&data[..8]);
    Ok(discriminator)
}

// ============================================================================
// Performance Optimization #2: Zero-Copy Byte Reading Helpers
// ============================================================================
// These helpers read primitive types directly from byte slices without
// deserializing into intermediate structs. This avoids allocation overhead.

/// Read a u64 from a byte slice at the given offset (little-endian).
/// Returns None if there aren't enough bytes.
#[inline]
pub fn read_u64_le(data: &[u8], offset: usize) -> Option<u64> {
    if data.len() < offset + 8 {
        return None;
    }
    let bytes: [u8; 8] = data[offset..offset + 8].try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

/// Read an i64 from a byte slice at the given offset (little-endian).
/// Returns None if there aren't enough bytes.
#[inline]
pub fn read_i64_le(data: &[u8], offset: usize) -> Option<i64> {
    if data.len() < offset + 8 {
        return None;
    }
    let bytes: [u8; 8] = data[offset..offset + 8].try_into().ok()?;
    Some(i64::from_le_bytes(bytes))
}

/// Read a Pubkey (32 bytes) from a byte slice at the given offset.
/// Returns None if there aren't enough bytes.
#[inline]
pub fn read_pubkey(data: &[u8], offset: usize) -> Option<Pubkey> {
    if data.len() < offset + 32 {
        return None;
    }
    let bytes: [u8; 32] = data[offset..offset + 32].try_into().ok()?;
    Some(Pubkey::from(bytes))
}

/// Read a bool from a byte slice at the given offset.
/// Returns None if there aren't enough bytes.
#[inline]
pub fn read_bool(data: &[u8], offset: usize) -> Option<bool> {
    data.get(offset).map(|&b| b != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_get_discriminator() {
        let data = vec![1, 2, 3, 4];
        assert_eq!(get_discriminator(&data).unwrap(), 1);
        
        let empty_data = vec![];
        assert!(get_discriminator(&empty_data).is_err());
    }
    
    #[test]
    fn test_get_anchor_discriminator() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let discriminator = get_anchor_discriminator(&data).unwrap();
        assert_eq!(discriminator, [1, 2, 3, 4, 5, 6, 7, 8]);
        
        let short_data = vec![1, 2, 3];
        assert!(get_anchor_discriminator(&short_data).is_err());
    }
}
