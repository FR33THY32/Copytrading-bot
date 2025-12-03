# Copy Trading Bot Configuration Guide

This guide explains how to configure the bot using the `.env` file. All settings are managed through this file, which must be in the same directory as the bot binary.

## Setup

1. Create a new file named `.env` in the same folder as the bot.
2. Copy the configuration options below and adjust them to your needs.

## Required Settings

These settings are mandatory for the bot to function.

| Setting | Description | Example |
|---------|-------------|---------|
| `PRIVATE_KEY` | Your wallet's private key (Base58 format). This wallet will execute the trades. | `5M...` |
| `RPC_URL` | Solana RPC URL (HTTP). Used for fetching account data and balances. | `https://mainnet.helius-rpc.com/?api-key=...` |
| `GRPC_ENDPOINT` | Yellowstone gRPC Endpoint. Used for real-time transaction monitoring. | `http://grpc.helius-rpc.com:10000` |
| `GRPC_X_TOKEN` | Authentication token for the gRPC endpoint (if required by your provider). | `your-api-token` |

## Trading Settings

Control how much to buy and how much to pay for transaction speed.

| Setting | Description | Example |
|---------|-------------|---------|
| `BUY_AMOUNT_SOL` | Amount of SOL to spend on each buy order. | `0.1` |
| `PRIORITY_FEES` | Default priority fee (compute unit price) in SOL. Higher values help land transactions during congestion. | `0.005` |
| `TX_TIP` | Default tip (bribe) in SOL to pay to processors. Essential for landing transactions. | `0.002` | NEVER USE LESS THAN 0.001 SOL
| `BUY_PRIORITY_FEES` | (Optional) Override priority fee specifically for **Buy** transactions. | `0.01` |
| `SELL_PRIORITY_FEES` | (Optional) Override priority fee specifically for **Sell** transactions. | `0.005` |
| `BUY_TX_TIP` | (Optional) Override tip specifically for **Buy** transactions. | `0.005` | NEVER USE LESS THAN 0.001 SOL
| `SELL_TX_TIP` | (Optional) Override  tip specifically for **Sell** transactions. | `0.002` | NEVER USE LESS THAN 0.001 SOL

## Transaction Processors (Premium Endpoints)

Configure your API keys for the transaction execution services. You can use multiple services simultaneously for better reliability.

| Setting | Description |
|---------|-------------|
| `ASTRALANE_API_KEY` | API key for Astralane. |
| `BLOCKRAZOR_API_KEY` | API key for Blockrazor. |
| `STELLIUM_API_KEY` | API key for Stellium. |
| `FLASHBLOCK_API_KEY` | API key for Flashblock. |
| `ZERO_SLOT_API_KEY` | API key for Zero Slot. |

*Note: Leave the value empty (e.g., `ASTRALANE_API_KEY=`) if you do not have a key for a specific provider.*

## Target Wallets

Define the wallets you want to copy trade. You can add multiple targets by incrementing the number (1, 2, 3, etc.).

### Target 1 Configuration
```env
TARGET_WALLET1=TargetWalletAddressHere
TARGET_WALLET1_SLIPPAGE_PCT=15
TARGET_WALLET1_MIRROR_SELLS=true
TARGET_WALLET1_TAKE_PROFIT=50
TARGET_WALLET1_STOP_LOSS=20
```

### Target 2 Configuration (Optional)
```env
TARGET_WALLET2=AnotherWalletAddress
TARGET_WALLET2_SLIPPAGE_PCT=10
TARGET_WALLET2_MIRROR_SELLS=false
TARGET_WALLET2_TAKE_PROFIT=30
TARGET_WALLET2_STOP_LOSS=10
```

| Setting | Description |
|---------|-------------|
| `TARGET_WALLETx` | The public key of the wallet to copy. |
| `TARGET_WALLETx_SLIPPAGE_PCT` | Slippage tolerance in percentage (e.g., `15` for 15%). |
| `TARGET_WALLETx_MIRROR_SELLS` | `true`: Sell when the target sells.<br>`false`: Ignore target sells and rely only on your TP/SL settings. |
| `TARGET_WALLETx_TAKE_PROFIT` | (Optional) Auto-sell if price increases by this percentage. Set to `0` or remove to disable. |
| `TARGET_WALLETx_STOP_LOSS` | (Optional) Auto-sell if price decreases by this percentage. Set to `0` or remove to disable. |

## : Nonce Accounts

Nonce accounts allow the bot to send multiple transactions in parallel without them conflicting.

The bot automatically manages these. You only need to define empty lines for as many parallel transactions as you want to support (2-4 is recommended).

```env
NONCE_ACCOUNT_1=
NONCE_ACCOUNT_2=
NONCE_ACCOUNT_3=
```

*On the first run, the bot will automatically create these accounts on-chain (costing a tiny amount of SOL for rent) and write their addresses back to your .env file.*

## Example Complete .env

```env
PRIVATE_KEY=5M...
RPC_URL=https://mainnet.helius-rpc.com/?api-key=your-key
GRPC_ENDPOINT=http://grpc.helius-rpc.com:10000
GRPC_X_TOKEN=your-token

# Trading
BUY_AMOUNT_SOL=0.1
PRIORITY_FEES=0.001
TX_TIP=0.002

# Processors
BLOCKRAZOR_API_KEY=br_...
ASTRALANE_API_KEY=
STELLIUM_API_KEY=
FLASHBLOCK_API_KEY=
ZERO_SLOT_API_KEY=

# Target 1
TARGET_WALLET1=Hv2...TargetPubKey
TARGET_WALLET1_SLIPPAGE_PCT=10
TARGET_WALLET1_MIRROR_SELLS=true

# Nonce Accounts (Leave empty initially)
NONCE_ACCOUNT_1=
NONCE_ACCOUNT_2=
NONCE_ACCOUNT_3=
```
