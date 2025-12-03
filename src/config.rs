use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use base64::{engine::general_purpose, Engine as _};
use dotenvy::Error as DotenvError;
use serde::Deserialize;
use solana_sdk::{
    native_token::sol_to_lamports,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use thiserror::Error;

#[derive(Clone)]
pub struct Config {
    pub env_path: PathBuf,
    pub operator: Arc<Keypair>,
    pub buy_amount_sol: f64,
    pub buy_priority_fees: f64,
    pub sell_priority_fees: f64,
    pub buy_tx_tip_sol: f64,
    pub sell_tx_tip_sol: f64,
    pub astralane_api_key: String,
    pub blockrazor_api_key: String,
    pub stellium_api_key: String,
    pub flashblock_api_key: String,
    pub zero_slot_api_key: String,
    pub nozomi_api_key: String,
    pub nonce_accounts: Vec<Option<Pubkey>>,
    pub grpc_endpoint: String,
    pub grpc_x_token: Option<String>,
    pub rpc_url: String,
    pub target_wallets: Vec<TargetWalletConfig>,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let env_path = env::current_dir()
            .map_err(|e| ConfigError::Io("current_dir".into(), e))?
            .join(".env");

        match dotenvy::from_path(&env_path) {
            Ok(_) => {}
            Err(DotenvError::LineParse(_, _)) | Err(DotenvError::Io(_)) if env_path.exists() => {
                return Err(ConfigError::Dotenv)
            }
            Err(_) => {
                return Err(ConfigError::MissingEnv(env_path));
            }
        }

        let raw = RawConfig::gather()?;

        let operator = Arc::new(parse_keypair(&raw.private_key)?);

        let mut nonce_accounts = Vec::new();
        let mut i = 1;
        loop {
            let key = format!("NONCE_ACCOUNT_{}", i);
            match env::var(&key) {
                Ok(val) => {
                    nonce_accounts.push(parse_optional_pubkey(Some(&val))?);
                    i += 1;
                }
                Err(_) => break,
            }
        }

        let target_wallets = load_target_wallets()?;

        Ok(Self {
            env_path,
            operator,
            buy_amount_sol: raw.buy_amount_sol,
            buy_priority_fees: raw.buy_priority_fees,
            sell_priority_fees: raw.sell_priority_fees,
            buy_tx_tip_sol: raw.buy_tx_tip,
            sell_tx_tip_sol: raw.sell_tx_tip,
            astralane_api_key: raw.astralane_api_key,
            blockrazor_api_key: raw.blockrazor_api_key,
            stellium_api_key: raw.stellium_api_key,
            flashblock_api_key: raw.flashblock_api_key,
            zero_slot_api_key: raw.zero_slot_api_key,
            nozomi_api_key: raw.nozomi_api_key,
            nonce_accounts,
            grpc_endpoint: raw.grpc_endpoint,
            grpc_x_token: raw.grpc_x_token,
            rpc_url: raw.rpc_url,
            target_wallets,
        })
    }

    pub fn buy_amount_lamports(&self) -> u64 {
        sol_to_lamports(self.buy_amount_sol.max(0.0))
    }

    pub fn operator_pubkey(&self) -> Pubkey {
        self.operator.pubkey()
    }

    pub fn operator_keypair(&self) -> Arc<Keypair> {
        Arc::clone(&self.operator)
    }

    pub fn buy_tx_tip_lamports(&self) -> u64 {
        sol_to_lamports(self.buy_tx_tip_sol.max(0.0))
    }

    pub fn sell_tx_tip_lamports(&self) -> u64 {
        sol_to_lamports(self.sell_tx_tip_sol.max(0.0))
    }

    pub fn buy_compute_unit_price_microlamports(&self, cu_limit: u32) -> u64 {
        compute_unit_price_for_fee(self.buy_priority_fees, cu_limit)
    }

    pub fn sell_compute_unit_price_microlamports(&self, cu_limit: u32) -> u64 {
        compute_unit_price_for_fee(self.sell_priority_fees, cu_limit)
    }

    /// Check if a given processor endpoint is enabled (has API key configured).
    ///
    /// Helius and StandardRpc are always enabled.
    /// Other processors require their respective API keys to be set.
    pub fn is_processor_enabled(&self, processor: &str) -> bool {
        match processor {
            "Helius" => true,
            "StandardRpc" => !self.rpc_url.trim().is_empty(),
            "Astralane" => !self.astralane_api_key.trim().is_empty(),
            "Blockrazor" => !self.blockrazor_api_key.trim().is_empty(),
            "Stellium" => !self.stellium_api_key.trim().is_empty(),
            "Flashblock" => !self.flashblock_api_key.trim().is_empty(),
            "ZeroSlot" => !self.zero_slot_api_key.trim().is_empty(),
            "Nozomi" => !self.nozomi_api_key.trim().is_empty(),
            _ => false,
        }
    }
}

fn compute_unit_price_for_fee(fee: f64, cu_limit: u32) -> u64 {
    if cu_limit == 0 {
        return 0;
    }
    let micro_total = fee.max(0.0) * 1_000_000_000_000_000.0; // 1e15 microlamports
    (micro_total / cu_limit as f64)
        .max(0.0)
        .min(u64::MAX as f64) as u64
}

#[derive(Clone, Debug)]
pub struct TargetWalletConfig {
    pub wallet: Pubkey,
    pub slippage_pct: f64,
    pub mirror_sells: bool,
    pub take_profit_pct: Option<f64>,
    pub stop_loss_pct: Option<f64>,
}

#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "PRIVATE_KEY")]
    private_key: String,
    #[serde(rename = "BUY_AMOUNT_SOL", deserialize_with = "de_f64")]
    buy_amount_sol: f64,
    #[serde(rename = "BUY_PRIORITY_FEES", deserialize_with = "de_f64")]
    buy_priority_fees: f64,
    #[serde(rename = "SELL_PRIORITY_FEES", deserialize_with = "de_f64")]
    sell_priority_fees: f64,
    #[serde(rename = "BUY_TX_TIP", deserialize_with = "de_f64")]
    buy_tx_tip: f64,
    #[serde(rename = "SELL_TX_TIP", deserialize_with = "de_f64")]
    sell_tx_tip: f64,
    #[serde(rename = "ASTRALANE_API_KEY")]
    astralane_api_key: String,
    #[serde(rename = "BLOCKRAZOR_API_KEY")]
    blockrazor_api_key: String,
    #[serde(rename = "STELLIUM_API_KEY")]
    stellium_api_key: String,
    #[serde(rename = "FLASHBLOCK_API_KEY")]
    flashblock_api_key: String,
    #[serde(rename = "ZERO_SLOT_API_KEY")]
    zero_slot_api_key: String,
    #[serde(rename = "NOZOMI_API_KEY")]
    nozomi_api_key: String,
    #[serde(rename = "GRPC_ENDPOINT")]
    grpc_endpoint: String,
    #[serde(
        rename = "GRPC_X_TOKEN",
        default,
        deserialize_with = "de_optional_string"
    )]
    grpc_x_token: Option<String>,
    #[serde(rename = "RPC_URL")]
    rpc_url: String,
}

impl RawConfig {
    fn gather() -> Result<Self, ConfigError> {
        let mut data = std::collections::BTreeMap::new();
        for (key, value) in env::vars() {
            data.insert(key, value);
        }
        let json = serde_json::to_value(&data).map_err(|e| ConfigError::Serde(e.to_string()))?;
        serde_json::from_value(json).map_err(|e| ConfigError::Serde(e.to_string()))
    }
}

fn parse_optional_pubkey(value: Option<&str>) -> Result<Option<Pubkey>, ConfigError> {
    match value {
        Some(v) if !v.trim().is_empty() => Pubkey::from_str(v.trim())
            .map(Some)
            .map_err(|e| ConfigError::Pubkey(v.into(), e)),
        _ => Ok(None),
    }
}

fn parse_keypair(encoded: &str) -> Result<Keypair, ConfigError> {
    let trimmed = encoded.trim();

    if let Ok(bytes) = bs58::decode(trimmed).into_vec() {
        if let Ok(kp) = Keypair::from_bytes(&bytes) {
            return Ok(kp);
        }
    }

    if let Ok(bytes) = general_purpose::STANDARD.decode(trimmed.as_bytes()) {
        if let Ok(kp) = Keypair::from_bytes(&bytes) {
            return Ok(kp);
        }
    }

    if trimmed.starts_with('[') {
        if let Ok(vec) = serde_json::from_str::<Vec<u8>>(trimmed) {
            if let Ok(kp) = Keypair::from_bytes(&vec) {
                return Ok(kp);
            }
        }
    }

    Err(ConfigError::InvalidPrivateKey)
}

fn parse_bool(key: &str, raw: &str) -> Result<bool, ConfigError> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => Err(ConfigError::InvalidBoolean {
            key: key.to_string(),
            value: raw.to_string(),
        }),
    }
}

fn parse_optional_percentage(key: &str, raw: &str) -> Result<Option<f64>, ConfigError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value = trimmed
        .parse::<f64>()
        .map_err(|_| ConfigError::InvalidPercentage {
            key: key.to_string(),
            value: raw.to_string(),
        })?;
    if value <= 0.0 {
        if value < 0.0 {
            return Err(ConfigError::InvalidPercentage {
                key: key.to_string(),
                value: raw.to_string(),
            });
        }
        return Ok(None);
    }
    Ok(Some(value))
}

fn de_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    raw.trim()
        .parse::<f64>()
        .map_err(|_| serde::de::Error::custom("expected number"))
}

fn de_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }))
}

fn de_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(serde::de::Error::custom("expected number"));
        }
        trimmed
            .parse::<f64>()
            .map_err(|_| serde::de::Error::custom("expected number"))
    })
    .transpose()
}

fn load_target_wallets() -> Result<Vec<TargetWalletConfig>, ConfigError> {
    let mut wallets = Vec::new();
    let mut index = 1;

    loop {
        let wallet_key = format!("TARGET_WALLET{index}");
        let wallet_value = match env::var(&wallet_key) {
            Ok(value) => value,
            Err(env::VarError::NotPresent) => break,
            Err(err) => return Err(ConfigError::EnvVar(wallet_key, err)),
        };

        let slippage_key = format!("{}_SLIPPAGE_PCT", wallet_key);
        let slippage_value = match env::var(&slippage_key) {
            Ok(value) => value,
            Err(env::VarError::NotPresent) => {
                return Err(ConfigError::MissingTargetField(slippage_key));
            }
            Err(err) => return Err(ConfigError::EnvVar(slippage_key, err)),
        };

        let wallet_pubkey = Pubkey::from_str(wallet_value.trim())
            .map_err(|e| ConfigError::Pubkey(wallet_value.clone(), e))?;
        let slippage_pct = slippage_value
            .trim()
            .parse::<f64>()
            .map_err(|_| ConfigError::InvalidSlippage(slippage_value.clone()))?;

        let mirror_key = format!("{}_MIRROR_SELLS", wallet_key);
        let mirror_sells = match env::var(&mirror_key) {
            Ok(value) => parse_bool(&mirror_key, &value)?,
            Err(env::VarError::NotPresent) => true,
            Err(err) => return Err(ConfigError::EnvVar(mirror_key, err)),
        };

        let take_profit_key = format!("{}_TAKE_PROFIT", wallet_key);
        let take_profit_pct = match env::var(&take_profit_key) {
            Ok(value) => parse_optional_percentage(&take_profit_key, &value)?,
            Err(env::VarError::NotPresent) => None,
            Err(err) => return Err(ConfigError::EnvVar(take_profit_key, err)),
        };

        let stop_loss_key = format!("{}_STOP_LOSS", wallet_key);
        let stop_loss_pct = match env::var(&stop_loss_key) {
            Ok(value) => parse_optional_percentage(&stop_loss_key, &value)?,
            Err(env::VarError::NotPresent) => None,
            Err(err) => return Err(ConfigError::EnvVar(stop_loss_key, err)),
        };

        wallets.push(TargetWalletConfig {
            wallet: wallet_pubkey,
            slippage_pct,
            mirror_sells,
            take_profit_pct,
            stop_loss_pct,
        });

        index += 1;
    }

    Ok(wallets)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> Config {
        Config {
            env_path: PathBuf::new(),
            operator: Arc::new(Keypair::new()),
            buy_amount_sol: 1.0,
            buy_priority_fees: 0.003,
            sell_priority_fees: 0.004,
            buy_tx_tip_sol: 0.005,
            sell_tx_tip_sol: 0.006,
            astralane_api_key: String::new(),
            blockrazor_api_key: String::new(),
            stellium_api_key: String::new(),
            flashblock_api_key: String::new(),
            zero_slot_api_key: String::new(),
            nozomi_api_key: String::new(),
            nonce_accounts: vec![],
            grpc_endpoint: String::new(),
            grpc_x_token: None,
            rpc_url: String::new(),
            target_wallets: vec![],
        }
    }

    #[test]
    fn per_side_fee_helpers() {
        let config = sample_config();
        assert_eq!(
            config.buy_compute_unit_price_microlamports(100_000),
            super::compute_unit_price_for_fee(0.003, 100_000)
        );
        assert_eq!(
            config.sell_compute_unit_price_microlamports(200_000),
            super::compute_unit_price_for_fee(0.004, 200_000)
        );
        assert_eq!(config.buy_tx_tip_lamports(), sol_to_lamports(0.005));
        assert_eq!(config.sell_tx_tip_lamports(), sol_to_lamports(0.006));
    }

    #[test]
    fn processor_enabled_checks() {
        // Start with empty config - only Helius should be enabled (no API key needed)
        let mut config = sample_config();

        // Helius is always enabled
        assert!(config.is_processor_enabled("Helius"));

        // StandardRpc requires rpc_url
        assert!(!config.is_processor_enabled("StandardRpc"));
        config.rpc_url = "http://example.com".to_string();
        assert!(config.is_processor_enabled("StandardRpc"));

        // Other processors require their respective API keys
        assert!(!config.is_processor_enabled("Astralane"));
        config.astralane_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("Astralane"));

        assert!(!config.is_processor_enabled("Blockrazor"));
        config.blockrazor_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("Blockrazor"));

        assert!(!config.is_processor_enabled("Stellium"));
        config.stellium_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("Stellium"));

        assert!(!config.is_processor_enabled("Flashblock"));
        config.flashblock_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("Flashblock"));

        assert!(!config.is_processor_enabled("ZeroSlot"));
        config.zero_slot_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("ZeroSlot"));

        assert!(!config.is_processor_enabled("Nozomi"));
        config.nozomi_api_key = "test-key".to_string();
        assert!(config.is_processor_enabled("Nozomi"));

        // Unknown processor should return false
        assert!(!config.is_processor_enabled("Unknown"));
    }

    #[test]
    fn processor_enabled_ignores_whitespace() {
        let mut config = sample_config();

        // Whitespace-only keys should not enable processor
        config.astralane_api_key = "   ".to_string();
        assert!(!config.is_processor_enabled("Astralane"));

        config.astralane_api_key = "\t\n".to_string();
        assert!(!config.is_processor_enabled("Astralane"));

        // Key with actual content should enable
        config.astralane_api_key = "  valid-key  ".to_string();
        assert!(config.is_processor_enabled("Astralane"));
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("could not determine working directory for {0}")]
    Io(String, #[source] std::io::Error),
    #[error("missing .env at {0}")]
    MissingEnv(PathBuf),
    #[error("failed to parse .env file")]
    Dotenv,
    #[error("invalid private key")]
    InvalidPrivateKey,
    #[error("pubkey parse error for {0}")]
    Pubkey(String, #[source] solana_sdk::pubkey::ParsePubkeyError),
    #[error("serialization error: {0}")]
    Serde(String),
    #[error("env var {0} error")]
    EnvVar(String, env::VarError),
    #[error("missing target wallet field {0}")]
    MissingTargetField(String),
    #[error("invalid slippage percentage: {0}")]
    InvalidSlippage(String),
    #[error("invalid boolean value {value} for {key}")]
    InvalidBoolean { key: String, value: String },
    #[error("invalid percentage value {value} for {key}")]
    InvalidPercentage { key: String, value: String },
}

impl ConfigError {
    pub fn missing_env_path(&self) -> Option<&Path> {
        match self {
            ConfigError::MissingEnv(path) => Some(path.as_path()),
            _ => None,
        }
    }
}
