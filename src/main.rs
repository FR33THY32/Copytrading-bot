mod async_log;
mod config;
mod copytrade;
mod executor;
mod lazy_signature;
mod nonce;
mod parsers;
mod program_registry;
mod serialization;
mod swap;
mod transaction_processor;

use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use {
    futures::{sink::SinkExt, stream::StreamExt},
    solana_rpc_client::rpc_client::RpcClient,
    std::{collections::HashMap, env, sync::Arc, time::Duration},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        geyser::SubscribeRequestFilterTransactions,
        prelude::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
            SubscribeUpdateTransaction,
        },
    },
};

use crate::{
    config::Config,
    copytrade::{ConfirmationTracker, CopyTradeRuntime, CopyTrader, GrpcDump, TraderMessage},
    executor::ExecutionPipeline,
    nonce::NonceManager,
    program_registry::ProgramRegistry,
    transaction_processor::{ParsedTransaction, TransactionProcessor},
};

type TxnFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;

const PUMP_FUN: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMP_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();
    let _async_logger = async_log::init_async_logger();

    let config = Arc::new(Config::load()?);
    log_startup_summary(&config);
    info!(
        "Operator {} ready | sol_balance={:.4} | rpc={} | grpc={}",
        config.operator_pubkey(),
        0.0,
        config.rpc_url,
        config.grpc_endpoint
    );

    // Pre-resolve DNS for all endpoints to reduce latency
    let resolved_dns = executor::resolve_all_endpoints().await;
    info!("Pre-resolved {} DNS entries", resolved_dns.len());

    // Create execution pipeline with pre-resolved DNS, warmers share the same HTTP client
    let execution_pipeline = Arc::new(ExecutionPipeline::with_resolved_dns(
        Arc::clone(&config),
        &resolved_dns,
    ));
    let _connection_warmers =
        executor::spawn_connection_warmers(Arc::clone(&config), execution_pipeline.client());

    // Create program registry with default parsers
    let registry = Arc::new(ProgramRegistry::with_defaults());
    info!("Created program registry with {} parsers", registry.len());

    let nonce_manager = Arc::new(NonceManager::new(Arc::clone(&config))?);
    info!(
        "Loaded {} nonce account(s) for operator",
        nonce_manager.nonce_accounts().len()
    );

    // Create transaction processor
    let processor = Arc::new(TransactionProcessor::new(registry));
    let confirmation_tracker = Arc::new(ConfirmationTracker::new());
    let copy_trade_runtime = Arc::new(CopyTradeRuntime::new(
        Arc::clone(&config),
        Arc::clone(&nonce_manager),
        Arc::clone(&execution_pipeline),
        Arc::clone(&confirmation_tracker),
    ));
    confirmation_tracker.attach_runtime(&copy_trade_runtime);

    let grpc_dump = match env::var("GRPC_DUMP") {
        Ok(value) if value.eq_ignore_ascii_case("true") => match GrpcDump::new_from_env() {
            Ok(dump) => {
                info!(
                    "gRPC dump enabled; writing failed target transactions to {}",
                    dump.dir().display()
                );
                Some(Arc::new(dump))
            }
            Err(err) => {
                warn!("Failed to initialize gRPC dump directory: {err:?}");
                None
            }
        },
        _ => None,
    };

    let (trader_tx, trader_rx) = mpsc::channel(1000);
    let trader = CopyTrader::new(
        Arc::clone(&config),
        Some(Arc::clone(&copy_trade_runtime)),
        grpc_dump.clone(),
    );
    tokio::spawn(trader.run(trader_rx));

    let client = connect_geyser(&config).await?;
    debug!("Connected");

    let request = build_subscribe_request()?;
    geyser_subscribe(client, request, processor, trader_tx, confirmation_tracker).await?;

    Ok(())
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
    processor: Arc<TransactionProcessor>,
    trader_tx: mpsc::Sender<TraderMessage>,
    confirmations: Arc<ConfirmationTracker>,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("gRPC stream opened, listening for Pump.fun / Pump AMM transactions...");

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Transaction(update)) => {
                    process_transaction_update(update, &processor, &trader_tx, &confirmations)
                        .await;
                }
                Some(UpdateOneof::Ping(_)) => {
                    subscribe_tx
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await?;
                }
                Some(UpdateOneof::Pong(_)) => {}
                None => {
                    error!("update not found in the message");
                    break;
                }
                _ => {}
            },
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }
    }

    debug!("stream closed");
    Ok(())
}

/// Process a transaction update from the gRPC stream
async fn process_transaction_update(
    update: SubscribeUpdateTransaction,
    processor: &Arc<TransactionProcessor>,
    trader_tx: &mpsc::Sender<TraderMessage>,
    confirmations: &Arc<ConfirmationTracker>,
) {
    // Extract transaction info
    let tx_info = match update.transaction.as_ref() {
        Some(info) => info,
        None => {
            error!("Transaction update missing transaction info");
            return;
        }
    };

    // Extract signature
    let signature = &tx_info.signature;
    if signature.is_empty() {
        error!("Transaction update missing signature");
        return;
    }

    let sig_str = bs58::encode(signature).into_string();
    debug!("Processing transaction: {}", sig_str);

    // Process the transaction
    match processor.process_yellowstone_transaction(signature, tx_info) {
        Ok(parsed_tx) => {
            // Log parsing performance metrics at debug to reduce noise
            let sig_str = bs58::encode(&parsed_tx.signature.raw()).into_string();
            debug!(
                "Transaction {} parsed in {}μs | Actions: {}",
                sig_str,
                parsed_tx.total_parsing_time_us,
                parsed_tx.actions.len()
            );
            log_parsed_details(&sig_str, &parsed_tx);
            let pending = confirmations.take(parsed_tx.signature.raw());
            if let Some(ref confirmation) = pending {
                let latency = confirmation.sent_at.elapsed();
                info!(
                    "{:?} transaction {} landed on Geyser in {:?}",
                    confirmation.protocol, sig_str, latency
                );
            }
            let message = TraderMessage::ProcessTransaction {
                parsed_tx: Box::new(parsed_tx),
                confirmation: pending,
                update: Some(update),
            };
            if let Err(err) = trader_tx.send(message).await {
                warn!("Failed to enqueue transaction for trader: {err}");
            }
        }
        Err(e) => {
            warn!(
                "Failed to parse transaction {}: {:?}",
                bs58::encode(signature).into_string(),
                e
            );
        }
    }
}

fn log_startup_summary(config: &Config) {
    let rpc = RpcClient::new_with_timeout(config.rpc_url.clone(), Duration::from_secs(10));
    let operator = config.operator_pubkey();
    let balance_lamports = match rpc.get_balance(&operator) {
        Ok(value) => value,
        Err(err) => {
            warn!("Failed to fetch operator SOL balance: {err}");
            0
        }
    };
    let balance_sol = balance_lamports as f64 / 1_000_000_000.0;

    info!(
        "Startup | operator={} | sol={:.4} | buy_sol={:.4} | buy_prio={:.4} | sell_prio={:.4} | buy_tip={:.6} | sell_tip={:.6}",
        operator, balance_sol, config.buy_amount_sol, config.buy_priority_fees, config.sell_priority_fees, config.buy_tx_tip_sol, config.sell_tx_tip_sol,
    );
    info!(
        "Endpoints | rpc={} | grpc={}",
        config.rpc_url, config.grpc_endpoint
    );

    if config.target_wallets.is_empty() {
        info!("Targets | none configured");
    } else {
        for (idx, target) in config.target_wallets.iter().enumerate() {
            info!(
                "Target {:02} | wallet={} | slippage={:.2}%",
                idx + 1,
                target.wallet,
                target.slippage_pct,
            );
        }
    }
}

fn log_parsed_details(signature: &str, parsed_tx: &ParsedTransaction) {
    use crate::parsers::ParsedAction;

    const MAX_ITEMS: usize = 3;

    if parsed_tx.actions.is_empty() {
        debug!("Transaction {} has no parsed actions", signature);
        return;
    }

    for (idx, action) in parsed_tx.actions.iter().take(MAX_ITEMS).enumerate() {
        match action {
            ParsedAction::PumpFun(op) => {
                debug!(
                    "Tx {} | Action {} PumpFun {:?} | event={}",
                    signature,
                    idx,
                    std::mem::discriminant(&op.instruction),
                    op.event.is_some()
                );
            }
            ParsedAction::PumpAmm(op) => {
                debug!(
                    "Tx {} | Action {} PumpAmm {:?} | event={}",
                    signature,
                    idx,
                    std::mem::discriminant(&op.instruction),
                    op.event.is_some()
                );
            }
        }
    }

    if parsed_tx.actions.len() > MAX_ITEMS {
        debug!(
            "Tx {} | … {} more actions omitted",
            signature,
            parsed_tx.actions.len() - MAX_ITEMS
        );
    }
}

async fn connect_geyser(config: &Config) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
    let mut builder = GeyserGrpcClient::build_from_shared(config.grpc_endpoint.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(1024 * 1024 * 1024)
        .tcp_nodelay(true);

    if let Some(token) = &config.grpc_x_token {
        builder = builder.x_token(Some(token.clone()))?;
    }

    builder.connect().await.map_err(Into::into)
}

fn build_subscribe_request() -> anyhow::Result<SubscribeRequest> {
    let mut transactions: TxnFilterMap = HashMap::new();

    transactions.insert(
        "client".to_owned(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None, // Include both successful and failed transactions
            account_include: vec![PUMP_FUN.to_string(), PUMP_AMM.to_string()],
            account_exclude: vec![],
            account_required: vec![],
            signature: None,
        },
    );

    Ok(SubscribeRequest {
        accounts: HashMap::default(),
        slots: HashMap::default(),
        transactions,
        transactions_status: HashMap::default(),
        blocks: HashMap::default(),
        blocks_meta: HashMap::default(),
        entry: HashMap::default(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: Vec::default(),
        ping: None,
        from_slot: None,
    })
}
