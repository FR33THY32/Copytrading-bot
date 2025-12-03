use std::{
    fmt,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use base64::{engine::general_purpose, Engine as _};
use log::{debug, info, warn};

use crate::info_async;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client,
};
use serde::Deserialize;
use serde_json::json;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use tokio::{net::lookup_host, task::JoinHandle, time::sleep};
use url::Url;

use crate::{
    config::Config,
    swap::constants::{
        ASTRALANE_TIP_ACCOUNTS, BLOCKRAZOR_TIP_ACCOUNTS, FLASH_BLOCK_TIP_ACCOUNTS,
        HELIUS_TIP_ACCOUNTS, NOZOMI_TIP_ACCOUNTS, STELLIUM_TIP_ACCOUNTS, ZERO_SLOT_TIP_ACCOUNTS,
    },
};

#[allow(dead_code)]
const HELIUS_SEND_ENDPOINT: &str = "http://fra-sender.helius-rpc.com/fast";
const HELIUS_PING_ENDPOINT: &str = "http://fra-sender.helius-rpc.com/ping";
const HELIUS_INTERVAL: Duration = Duration::from_secs(5);
#[allow(dead_code)]
const ASTRALANE_SEND_ENDPOINT: &str = "http://fr.gateway.astralane.io/iris2";
const ASTRALANE_HEALTH_ENDPOINT: &str = "http://fr.gateway.astralane.io/gethealth";
const ASTRALANE_INTERVAL: Duration = Duration::from_secs(5);
#[allow(dead_code)]
const BLOCKRAZOR_SEND_ENDPOINT: &str = "http://frankfurt.solana.blockrazor.xyz:443/sendTransaction";
const BLOCKRAZOR_HEALTH_ENDPOINT: &str = "http://frankfurt.solana.blockrazor.xyz:443/health";
const BLOCKRAZOR_INTERVAL: Duration = Duration::from_secs(5);
const STELLIUM_ENDPOINT: &str = "http://fra1.flashrpc.com";
const STELLIUM_INTERVAL: Duration = Duration::from_secs(5);
const FLASHBLOCK_ENDPOINT: &str = "http://fra.flashblock.trade";
const FLASHBLOCK_INTERVAL: Duration = Duration::from_secs(5);
const ZERO_SLOT_SEND_ENDPOINT: &str = "http://de1.0slot.trade";
const ZERO_SLOT_HEALTH_ENDPOINT: &str = "http://de1.0slot.trade/health";
const ZERO_SLOT_INTERVAL: Duration = Duration::from_secs(5);
const NOZOMI_SEND_ENDPOINT: &str = "http://fra2.nozomi.temporal.xyz/api/sendTransaction2";
const NOZOMI_PING_ENDPOINT: &str = "http://fra2.nozomi.temporal.xyz/ping";
const NOZOMI_INTERVAL: Duration = Duration::from_secs(10);
const RPC_HEALTH_INTERVAL: Duration = Duration::from_secs(5);
#[allow(dead_code)]
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Deserialize)]
struct HealthResponse {
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

pub fn spawn_connection_warmers(config: Arc<Config>, client: Client) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    handles.push(tokio::spawn(warm_helius(client.clone())));

    if !config.astralane_api_key.is_empty() {
        handles.push(tokio::spawn(warm_astralane(
            client.clone(),
            config.astralane_api_key.clone(),
        )));
    } else {
        info!("Astralane API key missing; skipping Astralane connection warmer");
    }

    if !config.blockrazor_api_key.is_empty() {
        handles.push(tokio::spawn(warm_blockrazor(
            client.clone(),
            config.blockrazor_api_key.clone(),
        )));
    } else {
        info!("Blockrazor API key missing; skipping Blockrazor connection warmer");
    }

    if !config.stellium_api_key.is_empty() {
        handles.push(tokio::spawn(warm_stellium(
            client.clone(),
            config.stellium_api_key.clone(),
        )));
    } else {
        info!("Stellium API key missing; skipping Stellium connection warmer");
    }

    if !config.flashblock_api_key.is_empty() {
        handles.push(tokio::spawn(warm_flashblock(client.clone())));
    } else {
        info!("Flashblock API key missing; skipping Flashblock connection warmer");
    }

    if !config.zero_slot_api_key.is_empty() {
        handles.push(tokio::spawn(warm_zero_slot(client.clone())));
    } else {
        info!("Zero Slot API key missing; skipping Zero Slot connection warmer");
    }

    if !config.nozomi_api_key.is_empty() {
        handles.push(tokio::spawn(warm_nozomi(client.clone())));
    } else {
        info!("Nozomi API key missing; skipping Nozomi connection warmer");
    }

    // Always warm the RPC connection
    handles.push(tokio::spawn(warm_rpc(client, config.rpc_url.clone())));

    handles
}

async fn warm_helius(client: Client) {
    loop {
        match client.get(HELIUS_PING_ENDPOINT).send().await {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Helius ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Helius ping request error: {err}"),
        }

        sleep(HELIUS_INTERVAL).await;
    }
}

async fn warm_astralane(client: Client, api_key: String) {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&api_key) {
        headers.insert("api_key", value);
    } else {
        warn!("Astralane API key contains invalid characters for header; skipping warmer");
        return;
    }

    loop {
        match client
            .get(ASTRALANE_HEALTH_ENDPOINT)
            .headers(headers.clone())
            .send()
            .await
        {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Astralane health ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Astralane health request error: {err}"),
        }

        sleep(ASTRALANE_INTERVAL).await;
    }
}

async fn warm_blockrazor(client: Client, api_key: String) {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    match HeaderValue::from_str(&api_key) {
        Ok(value) => {
            headers.insert("apikey", value);
        }
        Err(_) => {
            warn!("Blockrazor API key contains invalid characters; skipping warmer");
            return;
        }
    };

    loop {
        match client
            .get(BLOCKRAZOR_HEALTH_ENDPOINT)
            .headers(headers.clone())
            .send()
            .await
        {
            Ok(response) => match response.error_for_status() {
                Ok(resp) => {
                    if let Ok(body) = resp.text().await {
                        if let Ok(parsed) = serde_json::from_str::<HealthResponse>(&body) {
                            if let Some(status) = parsed.status.or(parsed.result) {
                                debug!("Blockrazor health: {}", status);
                            }
                        }
                    }
                }
                Err(err) => warn!(
                    "Blockrazor health failed (status {}): {err}",
                    err.status().unwrap_or_default()
                ),
            },
            Err(err) => warn!("Blockrazor health request error: {err}"),
        }

        sleep(BLOCKRAZOR_INTERVAL).await;
    }
}

async fn warm_stellium(client: Client, api_key: String) {
    let api_key = api_key.trim().to_owned();
    if api_key.is_empty() {
        warn!("Stellium API key missing; skipping warmer");
        return;
    }
    let url = format!("{STELLIUM_ENDPOINT}/{api_key}");
    loop {
        match client.get(&url).send().await {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Stellium warm ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Stellium warm request error: {err}"),
        }
        sleep(STELLIUM_INTERVAL).await;
    }
}

async fn warm_flashblock(client: Client) {
    loop {
        match client.get(FLASHBLOCK_ENDPOINT).send().await {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Flashblock warm ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Flashblock warm request error: {err}"),
        }
        sleep(FLASHBLOCK_INTERVAL).await;
    }
}

async fn warm_zero_slot(client: Client) {
    loop {
        match client.get(ZERO_SLOT_HEALTH_ENDPOINT).send().await {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Zero Slot warm ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Zero Slot warm request error: {err}"),
        }
        sleep(ZERO_SLOT_INTERVAL).await;
    }
}

async fn warm_nozomi(client: Client) {
    loop {
        match client.get(NOZOMI_PING_ENDPOINT).send().await {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "Nozomi warm ping failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("Nozomi warm request error: {err}"),
        }
        sleep(NOZOMI_INTERVAL).await;
    }
}

async fn warm_rpc(client: Client, rpc_url: String) {
    let rpc_url = rpc_url.trim().to_owned();
    if rpc_url.is_empty() {
        warn!("RPC URL missing; skipping RPC connection warmer");
        return;
    }

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getHealth"
    });

    loop {
        match client
            .post(&rpc_url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
        {
            Ok(response) => {
                if let Err(err) = response.error_for_status_ref() {
                    warn!(
                        "RPC health check failed (status {}): {err}",
                        err.status().unwrap_or_default()
                    );
                }
            }
            Err(err) => warn!("RPC health request error: {err}"),
        }
        sleep(RPC_HEALTH_INTERVAL).await;
    }
}

/// All endpoint URLs that need DNS pre-resolution
const ENDPOINTS_TO_RESOLVE: &[&str] = &[
    HELIUS_SEND_ENDPOINT,
    HELIUS_PING_ENDPOINT,
    ASTRALANE_SEND_ENDPOINT,
    ASTRALANE_HEALTH_ENDPOINT,
    BLOCKRAZOR_SEND_ENDPOINT,
    BLOCKRAZOR_HEALTH_ENDPOINT,
    STELLIUM_ENDPOINT,
    FLASHBLOCK_ENDPOINT,
    ZERO_SLOT_SEND_ENDPOINT,
    ZERO_SLOT_HEALTH_ENDPOINT,
    NOZOMI_SEND_ENDPOINT,
    NOZOMI_PING_ENDPOINT,
];

/// Extract hostname and port from a URL, defaulting to port 80 for http
fn extract_host_port(url_str: &str) -> Option<(String, u16)> {
    let url = Url::parse(url_str).ok()?;
    let host = url.host_str()?.to_string();
    let port = url
        .port()
        .unwrap_or(if url.scheme() == "https" { 443 } else { 80 });
    Some((host, port))
}

/// Pre-resolve DNS for all endpoints and return (hostname, port, resolved_addr) tuples
pub async fn resolve_all_endpoints() -> Vec<(String, u16, SocketAddr)> {
    let mut resolved = Vec::new();

    for endpoint in ENDPOINTS_TO_RESOLVE {
        if let Some((host, port)) = extract_host_port(endpoint) {
            let lookup_target = format!("{}:{}", host, port);
            let result = lookup_host(lookup_target.as_str()).await;
            match result {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        info!("DNS pre-resolved {} -> {}", host, addr);
                        resolved.push((host, port, addr));
                    } else {
                        warn!("DNS lookup returned no addresses for {}", host);
                    }
                }
                Err(err) => {
                    warn!("Failed to resolve DNS for {}: {}", host, err);
                }
            }
        }
    }

    resolved
}

fn build_http_client() -> Client {
    Client::builder()
        .tcp_nodelay(true)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        .pool_max_idle_per_host(8)
        .build()
        .expect("failed to build reqwest client")
}

/// Build HTTP client with pre-resolved DNS entries
pub fn build_http_client_with_dns(resolved: &[(String, u16, SocketAddr)]) -> Client {
    let mut builder = Client::builder()
        .tcp_nodelay(true)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        .pool_max_idle_per_host(8);

    for (host, _port, addr) in resolved {
        builder = builder.resolve(host, *addr);
    }

    builder.build().expect("failed to build reqwest client")
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct ExecutionPipeline {
    client: Client,
    config: Arc<Config>,
}

#[allow(dead_code)]
impl ExecutionPipeline {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            client: build_http_client(),
            config,
        }
    }

    /// Create pipeline with pre-resolved DNS for lower latency
    pub fn with_resolved_dns(config: Arc<Config>, resolved: &[(String, u16, SocketAddr)]) -> Self {
        Self {
            client: build_http_client_with_dns(resolved),
            config,
        }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn random_tip_address(processor: ProcessorEndpoint) -> Pubkey {
        match processor {
            ProcessorEndpoint::Helius => random_tip_from(&HELIUS_TIP_ACCOUNTS),
            ProcessorEndpoint::Astralane => random_tip_from(&ASTRALANE_TIP_ACCOUNTS),
            ProcessorEndpoint::Blockrazor => random_tip_from(&BLOCKRAZOR_TIP_ACCOUNTS),
            ProcessorEndpoint::Stellium => random_tip_from(&STELLIUM_TIP_ACCOUNTS),
            ProcessorEndpoint::Flashblock => random_tip_from(&FLASH_BLOCK_TIP_ACCOUNTS),
            ProcessorEndpoint::ZeroSlot => random_tip_from(&ZERO_SLOT_TIP_ACCOUNTS),
            ProcessorEndpoint::Nozomi => random_tip_from(&NOZOMI_TIP_ACCOUNTS),
            ProcessorEndpoint::StandardRpc => Pubkey::default(),
        }
    }

    pub fn encode_versioned_transaction(
        tx: &VersionedTransaction,
    ) -> Result<String, ExecutionError> {
        let bytes =
            bincode::serialize(tx).map_err(|err| ExecutionError::Serialization(err.to_string()))?;
        Ok(general_purpose::STANDARD.encode(bytes))
    }

    pub fn encode_bytes(bytes: &[u8]) -> String {
        general_purpose::STANDARD.encode(bytes)
    }

    pub async fn send_base64(
        &self,
        processor: ProcessorEndpoint,
        encoded_tx: &str,
    ) -> Result<(), ExecutionError> {
        match processor {
            ProcessorEndpoint::Helius => self.send_helius(encoded_tx).await,
            ProcessorEndpoint::Astralane => self.send_astralane(encoded_tx).await,
            ProcessorEndpoint::Blockrazor => self.send_blockrazor(encoded_tx).await,
            ProcessorEndpoint::Stellium => self.send_stellium(encoded_tx).await,
            ProcessorEndpoint::Flashblock => self.send_flashblock(encoded_tx).await,
            ProcessorEndpoint::ZeroSlot => self.send_zero_slot(encoded_tx).await,
            ProcessorEndpoint::Nozomi => self.send_nozomi(encoded_tx).await,
            ProcessorEndpoint::StandardRpc => self.send_rpc(encoded_tx).await,
        }
    }

    async fn send_helius(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let body = json!({
            "id": next_request_id("helius"),
            "jsonrpc": "2.0",
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });

        let response = self
            .client
            .post(HELIUS_SEND_ENDPOINT)
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Helius,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Helius sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_astralane(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.astralane_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingAstralaneKey);
        }

        let url = format!("{ASTRALANE_SEND_ENDPOINT}?api-key={api_key}&method=sendTransaction");

        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "text/plain")
            .body(encoded_tx.to_owned())
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Astralane,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Astralane sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_blockrazor(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.blockrazor_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingBlockrazorKey);
        }

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header =
            HeaderValue::from_str(&api_key).map_err(|_| ExecutionError::InvalidApiKey {
                processor: ProcessorEndpoint::Blockrazor,
            })?;
        headers.insert("apikey", api_key_header);

        let body = json!({
            "transaction": encoded_tx,
            "mode": "fast"
        });

        let response = self
            .client
            .post(BLOCKRAZOR_SEND_ENDPOINT)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Blockrazor,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Blockrazor sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_stellium(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.stellium_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingStelliumKey);
        }

        let url = format!("{STELLIUM_ENDPOINT}/{api_key}");
        let body = json!({
            "jsonrpc": "2.0",
            "id": next_request_id("stellium"),
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {
                    "encoding": "base64"
                }
            ]
        });

        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Stellium,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Stellium sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_flashblock(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.flashblock_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingFlashblockKey);
        }

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let auth_header =
            HeaderValue::from_str(&api_key).map_err(|_| ExecutionError::InvalidApiKey {
                processor: ProcessorEndpoint::Flashblock,
            })?;
        headers.insert("Authorization", auth_header);

        let body = json!({
            "jsonrpc": "2.0",
            "id": next_numeric_request_id(),
            "method": "sendTransaction",
            "params": [
                [encoded_tx],
                {
                    "encoding": "base64"
                }
            ]
        });

        let response = self
            .client
            .post(FLASHBLOCK_ENDPOINT)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Flashblock,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Flashblock sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_zero_slot(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.zero_slot_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingZeroSlotKey);
        }

        let url = format!("{}/?api-key={}", ZERO_SLOT_SEND_ENDPOINT, api_key);

        let body = json!({
            "jsonrpc": "2.0",
            "id": next_numeric_request_id(),
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {
                    "encoding": "base64"
                }
            ]
        });

        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::ZeroSlot,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "ZeroSlot sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_nozomi(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let api_key = self.config.nozomi_api_key.as_str().trim().to_owned();
        if api_key.is_empty() {
            return Err(ExecutionError::MissingNozomiKey);
        }

        let url = format!("{}?c={}", NOZOMI_SEND_ENDPOINT, api_key);

        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "text/plain")
            .body(encoded_tx.to_owned())
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::Nozomi,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "Nozomi sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }

    async fn send_rpc(&self, encoded_tx: &str) -> Result<(), ExecutionError> {
        let url = self.config.rpc_url.as_str().trim();
        if url.is_empty() {
            return Err(ExecutionError::MissingRpcUrl);
        }

        let body = json!({
            "jsonrpc": "2.0",
            "id": next_numeric_request_id(),
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });

        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(ExecutionError::Request)?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(ExecutionError::HttpResponse {
                processor: ProcessorEndpoint::StandardRpc,
                status: Some(status),
                body: text,
            });
        }

        info_async!(
            "StandardRpc sendTransaction response {} body {}",
            status.as_u16(),
            text
        );
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProcessorEndpoint {
    Helius,
    Astralane,
    Blockrazor,
    Stellium,
    Flashblock,
    ZeroSlot,
    Nozomi,
    StandardRpc,
}

impl ProcessorEndpoint {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProcessorEndpoint::Helius => "Helius",
            ProcessorEndpoint::Astralane => "Astralane",
            ProcessorEndpoint::Blockrazor => "Blockrazor",
            ProcessorEndpoint::Stellium => "Stellium",
            ProcessorEndpoint::Flashblock => "Flashblock",
            ProcessorEndpoint::ZeroSlot => "ZeroSlot",
            ProcessorEndpoint::Nozomi => "Nozomi",
            ProcessorEndpoint::StandardRpc => "StandardRpc",
        }
    }
}

impl fmt::Display for ProcessorEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[allow(dead_code)]
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Astralane API key missing")]
    MissingAstralaneKey,
    #[error("Blockrazor API key missing")]
    MissingBlockrazorKey,
    #[error("Stellium API key missing")]
    MissingStelliumKey,
    #[error("Flashblock API key missing")]
    MissingFlashblockKey,
    #[error("Zero Slot API key missing")]
    MissingZeroSlotKey,
    #[error("Nozomi API key missing")]
    MissingNozomiKey,
    #[error("Standard RPC URL missing")]
    MissingRpcUrl,
    #[error("{processor} API key contains invalid characters")]
    InvalidApiKey { processor: ProcessorEndpoint },
    #[error("{processor} returned HTTP error {status:?}: {body}")]
    HttpResponse {
        processor: ProcessorEndpoint,
        status: Option<reqwest::StatusCode>,
        body: String,
    },
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[allow(dead_code)]
fn random_tip_from(tips: &[Pubkey]) -> Pubkey {
    let mut rng = thread_rng();
    *tips
        .choose(&mut rng)
        .expect("tip account list should not be empty")
}

#[allow(dead_code)]
fn next_request_id(prefix: &str) -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{counter}")
}

fn next_numeric_request_id() -> u64 {
    REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed)
}
