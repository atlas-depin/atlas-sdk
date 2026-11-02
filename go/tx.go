package atlas

import (
	"context"
	"fmt"
	"time"

	"github.com/atlas-depin/atlas-sdk/go/internal/rpc"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
)

// BuyStorage signs and broadcasts a storage-purchase transaction.
func (c *Client) BuyStorage(ctx context.Context, wallet *Wallet, request BuyStorageRequest) (*TxResult, error) {
	if request.Duration <= 0 {
		return nil, fmt.Errorf("storage duration must be positive")
	}
	if request.Bytes <= 0 {
		return nil, fmt.Errorf("storage bytes must be positive")
	}
	if wallet == nil {
		return nil, fmt.Errorf("wallet is required")
	}
	return c.signAndBroadcast(ctx, wallet, &storagetypes.MsgBuyStorage{
		Creator:   wallet.Address(),
		Duration:  request.Duration,
		Bytes:     request.Bytes,
		IsDefault: request.IsDefault,
	})
}

func (c *Client) signAndBroadcast(ctx context.Context, wallet *Wallet, messages ...sdk.Msg) (*TxResult, error) {
	if wallet == nil {
		return nil, fmt.Errorf("wallet is required")
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("at least one message is required")
	}
	if err := c.requireGRPC("transaction signing"); err != nil {
		return nil, err
	}

	// Cosmos account sequences permit only one in-flight transaction per
	// account. Serializing here also makes a Client safe for concurrent callers.
	c.txMu.Lock()
	defer c.txMu.Unlock()

	accountNumber, sequence, err := c.accountSequence(ctx, wallet.Address())
	if err != nil {
		return nil, err
	}
	clientContext := c.clientContext.WithKeyring(wallet.keyring).WithFromName(wallet.keyName).WithFromAddress(wallet.address)
	txf := tx.Factory{}.
		WithTxConfig(clientContext.TxConfig).
		WithAccountRetriever(clientContext.AccountRetriever).
		WithChainID(c.config.ChainID).
		WithGasPrices(c.config.GasPrices).
		WithGasAdjustment(c.config.GasAdjustment).
		WithKeybase(wallet.keyring).
		WithAccountNumber(accountNumber).
		WithSequence(sequence).
		WithSignMode(signing.SignMode_SIGN_MODE_DIRECT).
		WithFromName(wallet.keyName)

	if c.config.GasLimit > 0 {
		txf = txf.WithGas(c.config.GasLimit)
	} else {
		_, gas, err := tx.CalculateGas(clientContext, txf.WithGas(250000), messages...)
		if err != nil {
			return nil, fmt.Errorf("simulate transaction gas: %w", err)
		}
		txf = txf.WithGas(gas)
	}

	txBuilder, err := txf.BuildUnsignedTx(messages...)
	if err != nil {
		return nil, fmt.Errorf("build transaction: %w", err)
	}
	if err := tx.Sign(ctx, txf, wallet.keyName, txBuilder, true); err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}
	txBytes, err := clientContext.TxConfig.TxEncoder()(txBuilder.GetTx())
	if err != nil {
		return nil, fmt.Errorf("encode transaction: %w", err)
	}
	return c.broadcastTxBytes(ctx, txBytes)
}

func (c *Client) accountSequence(ctx context.Context, address string) (uint64, uint64, error) {
	response, err := c.auth.Account(ctx, &authtypes.QueryAccountRequest{Address: address})
	if err != nil {
		return 0, 0, fmt.Errorf("query account %s: %w", address, err)
	}
	var account sdk.AccountI
	if err := c.registry.UnpackAny(response.Account, &account); err != nil {
		return 0, 0, fmt.Errorf("unpack account %s: %w", address, err)
	}
	return account.GetAccountNumber(), account.GetSequence(), nil
}

func (c *Client) broadcastTxBytes(ctx context.Context, txBytes []byte) (*TxResult, error) {
	if c.config.Transport == TransportRPC {
		result, err := c.rpc.BroadcastTxSync(ctx, txBytes)
		if err != nil {
			return nil, fmt.Errorf("broadcast transaction over RPC: %w", err)
		}
		return txResultFromRPC(result)
	}
	response, err := cosmostx.NewServiceClient(c.grpcConn).BroadcastTx(ctx, &cosmostx.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    cosmostx.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return nil, fmt.Errorf("broadcast transaction over gRPC: %w", err)
	}
	if response.TxResponse == nil {
		return nil, fmt.Errorf("broadcast transaction: empty response")
	}
	result := &TxResult{Hash: response.TxResponse.TxHash, Height: response.TxResponse.Height, Code: response.TxResponse.Code, Log: response.TxResponse.RawLog}
	if result.Code != 0 {
		return result, fmt.Errorf("transaction failed (%d): %s", result.Code, result.Log)
	}
	return result, nil
}

func txResultFromRPC(result *rpc.TxResult) (*TxResult, error) {
	if result == nil {
		return nil, fmt.Errorf("empty RPC transaction response")
	}
	converted := &TxResult{Hash: result.Hash, Height: result.Height, Code: result.Code, Log: result.Log}
	if converted.Code != 0 {
		return converted, fmt.Errorf("transaction failed (%d): %s", converted.Code, converted.Log)
	}
	return converted, nil
}

// WaitForTx waits for a transaction to be committed, using the configured
// broadcast transport's transaction lookup API.
func (c *Client) WaitForTx(ctx context.Context, hash string) (*TxResult, error) {
	if hash == "" {
		return nil, fmt.Errorf("transaction hash is required")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		result, err := c.lookupTx(ctx, hash)
		if err == nil {
			if result.Code != 0 {
				return result, fmt.Errorf("transaction failed (%d): %s", result.Code, result.Log)
			}
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) lookupTx(ctx context.Context, hash string) (*TxResult, error) {
	if c.config.Transport == TransportRPC {
		result, err := c.rpc.Tx(ctx, hash)
		if err != nil {
			return nil, err
		}
		return txResultFromRPC(result)
	}
	response, err := cosmostx.NewServiceClient(c.grpcConn).GetTx(ctx, &cosmostx.GetTxRequest{Hash: hash})
	if err != nil {
		return nil, err
	}
	if response.TxResponse == nil {
		return nil, fmt.Errorf("empty transaction response")
	}
	return &TxResult{Hash: response.TxResponse.TxHash, Height: response.TxResponse.Height, Code: response.TxResponse.Code, Log: response.TxResponse.RawLog}, nil
}
