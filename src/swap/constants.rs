use super::common::{
    calculate_with_slippage_buy, calculate_with_slippage_sell, ceil_div, compute_fee,
    sol_str_to_lamports,
};

use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;

pub const LOADED_ACCOUNTS_DATA_LIMIT: u32 = 15728640;

// Pump Fun has Constant Fees
pub const FEE_BASIS_POINTS: u64 = 125;

// Pump AMM Functions

/// Result for buying base tokens with quote amount input
#[derive(Clone, Debug)]
pub struct BuyQuoteInputResult {
    /// Amount of base tokens received
    pub base: u64,
    /// Effective quote amount after fee deduction
    pub internal_quote_without_fees: u64,
    /// Maximum quote amount with slippage protection
    pub max_quote: u64,
}

/// Result for selling base tokens with base amount input
#[derive(Clone, Debug)]
pub struct SellBaseInputResult {
    /// Final quote amount received after fees
    pub ui_quote: u64,
    /// Minimum quote amount with slippage protection
    pub min_quote: u64,
    /// Raw quote amount before fee deduction
    pub internal_quote_amount_out: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Fees {
    pub lp_fee_bps: u16,
    pub protocol_fee_bps: u16,
    pub coin_creator_fee_bps: u16,
}

/// Calculate base tokens received for a specific quote amount
///
/// # Arguments
/// * `quote` - Amount of quote tokens to spend
/// * `slippage_basis_points` - Slippage tolerance in basis points (100 = 1%)
/// * `base_reserve` - Base token reserves in the pool
/// * `quote_reserve` - Quote token reserves in the pool
/// * `coin_creator` - Token creator address
///
/// # Returns
/// * `BuyQuoteInputResult` containing base amount and slippage calculations
pub fn buy_quote_input_internal(
    quote: u64,
    slippage_basis_points: u64,
    base_reserve: u64,
    quote_reserve: u64,
    fees: Fees,
) -> Result<BuyQuoteInputResult, String> {
    if base_reserve == 0 || quote_reserve == 0 {
        return Err("Invalid input: 'baseReserve' or 'quoteReserve' cannot be zero.".to_string());
    }

    // Calculate total fee basis points
    let total_fee_bps = fees.lp_fee_bps + fees.protocol_fee_bps + fees.coin_creator_fee_bps;

    let denominator = 10_000 + total_fee_bps;

    // Calculate effective quote amount after fees
    let effective_quote = (quote as u128 * 10_000) / denominator as u128;

    // Calculate base amount out using constant product formula
    let numerator = (base_reserve as u128) * effective_quote;
    let denominator_effective = (quote_reserve as u128) + effective_quote;

    if denominator_effective == 0 {
        return Err("Pool would be depleted; denominator is zero.".to_string());
    }

    let base_amount_out = (numerator / denominator_effective) as u64;

    // Calculate max quote with slippage
    let max_quote = calculate_with_slippage_buy(quote, slippage_basis_points);

    Ok(BuyQuoteInputResult {
        base: base_amount_out,
        internal_quote_without_fees: effective_quote as u64,
        max_quote,
    })
}

/// Calculate quote tokens received for selling a specific amount of base tokens
///
/// # Arguments
/// * `base` - Amount of base tokens to sell
/// * `slippage_basis_points` - Slippage tolerance in basis points (100 = 1%)
/// * `base_reserve` - Base token reserves in the pool
/// * `quote_reserve` - Quote token reserves in the pool
/// * `coin_creator` - Token creator address
///
/// # Returns
/// * `SellBaseInputResult` containing quote amounts and slippage calculations
pub fn sell_base_input_internal(
    base: u64,
    slippage_basis_points: u64,
    base_reserve: u64,
    quote_reserve: u64,
    fees: Fees,
) -> Result<SellBaseInputResult, String> {
    if base_reserve == 0 || quote_reserve == 0 {
        return Err("Invalid input: 'baseReserve' or 'quoteReserve' cannot be zero.".to_string());
    }

    // Calculate quote amount out using constant product formula
    let quote_amount_out = ((quote_reserve as u128) * (base as u128)
        / ((base_reserve as u128) + (base as u128))) as u64;

    // Calculate fees
    let lp_fee = compute_fee(quote_amount_out as u128, fees.lp_fee_bps as u128) as u64;
    let protocol_fee = compute_fee(quote_amount_out as u128, fees.protocol_fee_bps as u128) as u64;
    let coin_creator_fee =
        compute_fee(quote_amount_out as u128, fees.coin_creator_fee_bps as u128) as u64;

    // Calculate final quote after fees
    let total_fees = lp_fee + protocol_fee + coin_creator_fee;
    if total_fees > quote_amount_out {
        return Err("Fees exceed total output; final quote is negative.".to_string());
    }
    let final_quote = quote_amount_out - total_fees;

    // Calculate min quote with slippage
    let min_quote = calculate_with_slippage_sell(final_quote, slippage_basis_points);

    Ok(SellBaseInputResult {
        ui_quote: final_quote,
        min_quote,
        internal_quote_amount_out: quote_amount_out,
    })
}

// Pump Fun Functions

/// Calculates the amount of tokens that can be purchased with a given SOL amount
/// using the bonding curve formula.
///
/// # Arguments
/// * `virtual_token_reserves` - Virtual token reserves in the bonding curve
/// * `virtual_sol_reserves` - Virtual SOL reserves in the bonding curve
/// * `real_token_reserves` - Actual token reserves available for purchase
/// * `amount` - SOL amount to spend (in lamports)
///
/// # Returns
/// The amount of tokens that will be received (in token's smallest unit)
pub fn get_buy_token_amount_from_sol_amount(
    virtual_token_reserves: u128,
    virtual_sol_reserves: u128,
    real_token_reserves: u128,
    amount: u64,
) -> u64 {
    if amount == 0 {
        return 0;
    }

    if virtual_token_reserves == 0 {
        return 0;
    }

    // Convert to u128 to prevent overflow
    let amount_128 = amount as u128;
    let total_fee_basis_points_128 = FEE_BASIS_POINTS as u128;

    let input_amount = amount_128
        .checked_mul(10_000)
        .unwrap()
        .checked_div(total_fee_basis_points_128 + 10_000)
        .unwrap();

    let denominator = virtual_sol_reserves + input_amount;

    let mut tokens_received = input_amount
        .checked_mul(virtual_token_reserves)
        .unwrap()
        .checked_div(denominator)
        .unwrap();

    tokens_received = tokens_received.min(real_token_reserves);

    if tokens_received <= 100 * 1_000_000_u128 {
        tokens_received = if amount > sol_str_to_lamports("0.01").unwrap_or(0) {
            25547619 * 1_000_000_u128
        } else {
            255476 * 1_000_000_u128
        };
    }

    tokens_received as u64
}

/// Calculates the amount of SOL that will be received when selling a given token amount
/// using the bonding curve formula with transaction fees deducted.
///
/// # Arguments
/// * `virtual_token_reserves` - Virtual token reserves in the bonding curve
/// * `virtual_sol_reserves` - Virtual SOL reserves in the bonding curve
/// * `creator` - Creator's public key (affects fee calculation)
/// * `amount` - Token amount to sell (in token's smallest unit)
///
/// # Returns
/// The amount of SOL that will be received after fees (in lamports)
pub fn get_sell_sol_amount_from_token_amount(
    virtual_token_reserves: u128,
    virtual_sol_reserves: u128,
    amount: u64,
) -> u64 {
    if amount == 0 {
        return 0;
    }

    // migrated bonding curve
    if virtual_token_reserves == 0 {
        return 0;
    }

    let amount_128 = amount as u128;

    // Calculate SOL amount received from selling tokens using constant product formula
    let numerator = amount_128.checked_mul(virtual_sol_reserves).unwrap_or(0);
    let denominator = virtual_token_reserves.checked_add(amount_128).unwrap_or(1);

    let sol_cost = numerator.checked_div(denominator).unwrap_or(0);

    let total_fee_basis_points_128 = FEE_BASIS_POINTS as u128;

    // Calculate transaction fee
    let fee = compute_fee(sol_cost, total_fee_basis_points_128);

    sol_cost.saturating_sub(fee) as u64
}

#[allow(dead_code)]
pub const HELIUS_TIP_ACCOUNTS: [Pubkey; 10] = [
    pubkey!("4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE"),
    pubkey!("D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ"),
    pubkey!("9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta"),
    pubkey!("5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn"),
    pubkey!("2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD"),
    pubkey!("2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ"),
    pubkey!("wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF"),
    pubkey!("3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT"),
    pubkey!("4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey"),
    pubkey!("4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or"),
];

#[allow(dead_code)]
pub const ASTRALANE_TIP_ACCOUNTS: [Pubkey; 8] = [
    pubkey!("astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF"),
    pubkey!("astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm"),
    pubkey!("astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk"),
    pubkey!("astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL"),
    pubkey!("astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK"),
    pubkey!("astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC"),
    pubkey!("astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk"),
    pubkey!("astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK"),
];

#[allow(dead_code)]
pub const BLOCKRAZOR_TIP_ACCOUNTS: [Pubkey; 14] = [
    pubkey!("FjmZZrFvhnqqb9ThCuMVnENaM3JGVuGWNyCAxRJcFpg9"),
    pubkey!("6No2i3aawzHsjtThw81iq1EXPJN6rh8eSJCLaYZfKDTG"),
    pubkey!("A9cWowVAiHe9pJfKAj3TJiN9VpbzMUq6E4kEvf5mUT22"),
    pubkey!("Gywj98ophM7GmkDdaWs4isqZnDdFCW7B46TXmKfvyqSm"),
    pubkey!("68Pwb4jS7eZATjDfhmTXgRJjCiZmw1L7Huy4HNpnxJ3o"),
    pubkey!("4ABhJh5rZPjv63RBJBuyWzBK3g9gWMUQdTZP2kiW31V9"),
    pubkey!("B2M4NG5eyZp5SBQrSdtemzk5TqVuaWGQnowGaCBt8GyM"),
    pubkey!("5jA59cXMKQqZAVdtopv8q3yyw9SYfiE3vUCbt7p8MfVf"),
    pubkey!("5YktoWygr1Bp9wiS1xtMtUki1PeYuuzuCF98tqwYxf61"),
    pubkey!("295Avbam4qGShBYK7E9H5Ldew4B3WyJGmgmXfiWdeeyV"),
    pubkey!("EDi4rSy2LZgKJX74mbLTFk4mxoTgT6F7HxxzG2HBAFyK"),
    pubkey!("BnGKHAC386n4Qmv9xtpBVbRaUTKixjBe3oagkPFKtoy6"),
    pubkey!("Dd7K2Fp7AtoN8xCghKDRmyqr5U169t48Tw5fEd3wT9mq"),
    pubkey!("AP6qExwrbRgBAVaehg4b5xHENX815sMabtBzUzVB4v8S"),
];

pub const STELLIUM_TIP_ACCOUNTS: [Pubkey; 5] = [
    pubkey!("ste11JV3MLMM7x7EJUM2sXcJC1H7F4jBLnP9a9PG8PH"),
    pubkey!("ste11MWPjXCRfQryCshzi86SGhuXjF4Lv6xMXD2AoSt"),
    pubkey!("ste11p5x8tJ53H1NbNQsRBg1YNRd4GcVpxtDw8PBpmb"),
    pubkey!("ste11p7e2KLYou5bwtt35H7BM6uMdo4pvioGjJXKFcN"),
    pubkey!("ste11TMV68LMi1BguM4RQujtbNCZvf1sjsASpqgAvSX"),
];

pub const FLASH_BLOCK_TIP_ACCOUNTS: [Pubkey; 10] = [
    pubkey!("FLaShB3iXXTWE1vu9wQsChUKq3HFtpMAhb8kAh1pf1wi"),
    pubkey!("FLashhsorBmM9dLpuq6qATawcpqk1Y2aqaZfkd48iT3W"),
    pubkey!("FLaSHJNm5dWYzEgnHJWWJP5ccu128Mu61NJLxUf7mUXU"),
    pubkey!("FLaSHR4Vv7sttd6TyDF4yR1bJyAxRwWKbohDytEMu3wL"),
    pubkey!("FLASHRzANfcAKDuQ3RXv9hbkBy4WVEKDzoAgxJ56DiE4"),
    pubkey!("FLasHstqx11M8W56zrSEqkCyhMCCpr6ze6Mjdvqope5s"),
    pubkey!("FLAShWTjcweNT4NSotpjpxAkwxUr2we3eXQGhpTVzRwy"),
    pubkey!("FLasHXTqrbNvpWFB6grN47HGZfK6pze9HLNTgbukfPSk"),
    pubkey!("FLAshyAyBcKb39KPxSzXcepiS8iDYUhDGwJcJDPX4g2B"),
    pubkey!("FLAsHZTRcf3Dy1APaz6j74ebdMC6Xx4g6i9YxjyrDybR"),
];

pub const ZERO_SLOT_TIP_ACCOUNTS: [Pubkey; 5] = [
    pubkey!("Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3"),
    pubkey!("FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe"),
    pubkey!("ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13"),
    pubkey!("6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK"),
    pubkey!("Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr"),
];

pub const NOZOMI_TIP_ACCOUNTS: [Pubkey; 17] = [
    pubkey!("TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq"),
    pubkey!("noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4"),
    pubkey!("noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE"),
    pubkey!("noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo"),
    pubkey!("noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ"),
    pubkey!("nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L"),
    pubkey!("nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z"),
    pubkey!("nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu"),
    pubkey!("noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7"),
    pubkey!("nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP"),
    pubkey!("nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P"),
    pubkey!("nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge"),
    pubkey!("nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3"),
    pubkey!("nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ"),
    pubkey!("nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk"),
    pubkey!("nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne"),
    pubkey!("nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb"),
];
