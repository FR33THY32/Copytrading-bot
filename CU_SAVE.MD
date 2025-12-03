# Manual Token Account Creation Strategy

This document outlines an alternative strategy for managing token accounts in the copy-trading bot. Instead of using the standard Associated Token Account (ATA) program, this strategy uses manual account creation with seeds.

## Motivation
- **Compute Unit (CU) Optimization**: Bypassing the Associated Token Program (ATP) Cross-Program Invocation (CPI) can slightly reduce CU usage.
- **Lifecycle Control**: Decoupling account creation and closing allows for "Async Closing," keeping the critical "Sell" transaction small and focused solely on the swap.

## Architectural Flow

### 1. Startup & State Management
- **Initialization**: On bot startup, query `getTokenAccountsByOwner` for the Operator wallet.
- **Cache**: Populate a local `HashMap<MintPubkey, AccountPubkey>` or `HashSet<MintPubkey>` with the existing token accounts.

### 2. Buy Logic (The "Conditional Create")
When processing a **Buy** signal:
1.  **Check Cache**: Look up the target Token Mint in the local cache.
2.  **If Exists**: 
    - Do **not** add any creation instructions to the transaction.
    - Use the cached address as the token account.
3.  **If Missing**:
    - Add `SystemInstruction::CreateAccountWithSeed` to the transaction.
    - Add `TokenInstruction::InitializeAccount3` to the transaction.
    - Update the cache (optimistically or upon confirmation) with the new address.

### 3. Sell Logic (Async Cleanup)
When processing a **Sell 100%** signal:
1.  **Critical Transaction**: Build and send the Swap transaction (Sell) *without* closing the account. This minimizes the transaction size and CU limit.
2.  **Cleanup Transaction**: Immediately after sending the sell, build a separate transaction containing `TokenInstruction::CloseAccount`.
    - Send this via `send_and_confirm_transaction`.
    - **On Confirmation**: Remove the account from the local Cache.

## Technical Implementation

### Account Derivation
Since these are not Canonical ATAs, we must derive the address manually using a seed.

```rust
// Example Derivation
let seed = &mint_pubkey.to_string()[..32]; // Use first 32 chars of mint string as seed
let token_account = Pubkey::create_with_seed(
    &operator_pubkey, // Base
    seed,             // Seed
    &token_program_id // Owner (Token Program)
)?;
```

### Instructions

**1. Create Account with Seed**
- **Program**: System Program
- **Params**:
    - `from`: Operator
    - `base`: Operator
    - `seed`: (As derived above)
    - `lamports`: ~2,039,280 (Rent exempt min for 165 bytes)
    - `space`: 165
    - `owner`: Token Program ID

**2. Initialize Account 3**
- **Program**: Token Program (or Token-2022)
- **Params**:
    - `account`: The derived token_account
    - `mint`: The token mint
    - `owner`: Operator

### Required Codebase Changes

1.  **`src/swap/pump.rs` & `pump_amm.rs`**:
    - Remove `ensure_user_matches` checks that enforce Canonical ATA derivation.
    - Replace `build_create_ata_instruction` with the 2-instruction sequence above.
    - Ensure builders accept the custom `token_account` address instead of deriving it internally.

2.  **`src/copytrade.rs`**:
    - Update `retarget_*` functions to use `Pubkey::create_with_seed` instead of `get_associated_token_address`.
    - Implement the `Cache` state in the `CopyTradeRuntime` or `Executor`.

## Risks & Mitigations

1.  **Non-Canonical ATAs**: 
    - *Issue*: Tokens in these accounts may not appear in standard wallets (Phantom/Solflare) or standard dApps (Jupiter) which look for the Canonical ATA.
    - *Mitigation*: This is acceptable for a pure bot wallet. If manual intervention is needed, tokens must be transferred to the ATA manually.

2.  **State Desync (Race Condition)**:
    - *Issue*: Buying a token immediately after firing the Async Close transaction. The Cache might say "Empty" (triggering a Create), but the Close hasn't landed yet, or the Account still exists on-chain.
    - *Mitigation*: Use `idempotent` creation if possible, or accept that the rare "Re-buy during pending close" might fail. Alternatively, queue the Buy to wait until the Close confirms.

