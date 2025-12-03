/// Calculate transaction fee based on amount and fee basis points
///
/// # Parameters
/// * `amount` - Transaction amount
/// * `fee_basis_points` - Fee basis points, 1 basis point = 0.01%
///
/// # Examples
/// * fee_basis_points = 1   -> 0.01% fee
/// * fee_basis_points = 10  -> 0.1% fee
/// * fee_basis_points = 25  -> 0.25% fee (common exchange rate)
/// * fee_basis_points = 100 -> 1% fee
pub fn compute_fee(amount: u128, fee_basis_points: u128) -> u128 {
    ceil_div(amount * fee_basis_points, 10_000)
}

/// Ceiling division implementation
/// Ceiling division that ensures results are not lost due to integer division precision
///
/// # Parameters
/// * `a` - Dividend
/// * `b` - Divisor
///
/// # Returns
/// Returns the ceiling result of a/b
pub fn ceil_div(a: u128, b: u128) -> u128 {
    (a + b - 1) / b
}

/// Calculate buy amount with slippage protection
/// Add slippage percentage to the amount to ensure successful purchase
///
/// # Parameters
/// * `amount` - Original transaction amount
/// * `basis_points` - Slippage basis points, 1 basis point = 0.01%
///
/// # Examples
/// * basis_points = 1   -> 0.01% slippage
/// * basis_points = 10  -> 0.1% slippage  
/// * basis_points = 100 -> 1% slippage
/// * basis_points = 500 -> 5% slippage
pub fn calculate_with_slippage_buy(amount: u64, basis_points: u64) -> u64 {
    amount + (amount * basis_points / 10000)
}

/// Calculate sell amount with slippage protection
/// Subtract slippage percentage from the amount to ensure successful sale
///
/// # Parameters
/// * `amount` - Original transaction amount
/// * `basis_points` - Slippage basis points, 1 basis point = 0.01%
///
/// # Examples
/// * basis_points = 1   -> 0.01% slippage
/// * basis_points = 10  -> 0.1% slippage  
/// * basis_points = 100 -> 1% slippage
/// * basis_points = 500 -> 5% slippage
pub fn calculate_with_slippage_sell(amount: u64, basis_points: u64) -> u64 {
    if amount <= basis_points / 10000 {
        1
    } else {
        amount - (amount * basis_points / 10000)
    }
}

/// Convert a decimal SOL string (e.g. "0.01") into lamports.
///
/// Returns None for invalid inputs, negative values, more than 9 fractional digits,
/// or on overflow.
///
/// Behavior mirrors the historical sol_str_to_lamports from solana_sdk:
/// - Up to 9 fractional digits are supported (1 SOL = 1_000_000_000 lamports).
/// - Fractional part is right-padded with zeros to 9 digits.
/// - Strings like ".5" are treated as 0.5; "1." is treated as 1.0.
pub fn sol_str_to_lamports(s: &str) -> Option<u64> {
    const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

    let mut s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Reject negatives; support optional leading "+"
    if s.starts_with('-') {
        return None;
    }
    if s.starts_with('+') {
        s = &s[1..];
        if s.is_empty() {
            return None;
        }
    }

    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        return None;
    }

    // Parse whole part (allow empty -> 0 to support strings like ".5")
    let whole_str = parts.get(0).copied().unwrap_or("");
    let whole: u64 = if whole_str.is_empty() {
        0
    } else {
        whole_str.parse().ok()?
    };

    // Parse fractional part, right-pad to 9 digits, reject >9 digits
    let frac_str = parts.get(1).copied().unwrap_or("");
    if frac_str.len() > 9 {
        return None;
    }
    let frac_padded = format!("{:0<9}", frac_str);
    let frac: u64 = if frac_str.is_empty() {
        0
    } else {
        frac_padded.parse().ok()?
    };

    let whole_lamports = whole.checked_mul(LAMPORTS_PER_SOL)?;
    whole_lamports.checked_add(frac)
}

pub fn priority_fees_per_cu_microlamports(priority_fees: f64, cu_limit: u32) -> u64 {
    if priority_fees <= 0.0 || cu_limit == 0 {
        return 0;
    }
    // 1 SOL = 1_000_000_000 lamports; 1 lamport = 1_000_000 microlamports => 1e15 microlamports per SOL
    let micro_total = priority_fees * 1_000_000_000_000_000.0; // 1e15
    let per_cu = micro_total / (cu_limit as f64);
    per_cu.max(0.0).min(u64::MAX as f64) as u64
}

/// Convert buy_amount from SOL to lamports
pub fn buy_amount_lamports(buy_amount: f64) -> u64 {
    let lamports = buy_amount * 1_000_000_000.0;
    lamports.max(0.0).min(u64::MAX as f64) as u64
}

const SOL_DECIMALS: u64 = 1_000_000_000;
const TOKEN_DECIMALS: u64 = 1_000_000;

pub fn compute_pump_fun_price(
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
) -> Option<f64> {
    if virtual_token_reserves == 0 {
        return None;
    }

    // Convert to f64 BEFORE dividing by decimals
    let v_sol_reserve = virtual_sol_reserves as f64 / SOL_DECIMALS as f64;
    let v_token_reserve = virtual_token_reserves as f64 / TOKEN_DECIMALS as f64;

    Some(v_sol_reserve / v_token_reserve)
}

pub fn compute_pump_amm_price(pool_base_reserve: u128, pool_quote_reserve: u128) -> f64 {
    if pool_base_reserve == 0 {
        return 0.0;
    }
    let scale = 10f64.powi(6 as i32); // 10^d
    (pool_quote_reserve as f64 * scale) / (pool_base_reserve as f64 * 1_000_000_000f64)
}
