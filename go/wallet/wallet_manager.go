package wallet

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	filetreetypes "github.com/atlas-depin/atlas/x/filetree/types"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	"github.com/cosmos/cosmos-sdk/client"
	clienttx "github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	sdk "github.com/cosmos/cosmos-sdk/types"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	vestingtypes "github.com/cosmos/cosmos-sdk/x/auth/vesting/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultGasPrices     = "0.025uatl"
	defaultGasAdjustment = 1.2
	defaultPollInterval  = time.Second
	defaultQueueCapacity = 32
)

// WalletManager signs and broadcasts Atlas transactions over gRPC. It is safe
// for concurrent use: submissions are serialized per Wallet, not globally.
type WalletManager struct {
	config WalletManagerConfig

	grpcConn *grpc.ClientConn
	client   client.Context
	codec    codec.Codec
	registry types.InterfaceRegistry
	auth     authtypes.QueryClient
	tx       cosmostx.ServiceClient

	queuesMu sync.Mutex
	queues   map[*Wallet]*walletQueue
	closed   bool
}

// NewWalletManager connects to an Atlas gRPC endpoint.
func NewWalletManager(ctx context.Context, cfg WalletManagerConfig) (*WalletManager, error) {
	if strings.TrimSpace(cfg.ChainID) == "" {
		return nil, fmt.Errorf("chain ID is required")
	}
	if strings.TrimSpace(cfg.GRPCAddress) == "" {
		return nil, fmt.Errorf("gRPC address is required")
	}
	if cfg.GasPrices == "" {
		cfg.GasPrices = defaultGasPrices
	}
	if _, err := sdk.ParseDecCoins(cfg.GasPrices); err != nil {
		return nil, fmt.Errorf("invalid gas prices %q: %w", cfg.GasPrices, err)
	}
	if cfg.GasAdjustment == 0 {
		cfg.GasAdjustment = defaultGasAdjustment
	}
	if cfg.GasAdjustment < 0 {
		return nil, fmt.Errorf("gas adjustment must be positive")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.PollInterval < 0 {
		return nil, fmt.Errorf("poll interval must be positive")
	}
	if cfg.QueueCapacity == 0 {
		cfg.QueueCapacity = defaultQueueCapacity
	}
	if cfg.QueueCapacity < -1 {
		return nil, fmt.Errorf("queue capacity must be positive or -1 for unbounded")
	}

	var transportCredentials credentials.TransportCredentials
	if cfg.GRPCInsecure {
		transportCredentials = insecure.NewCredentials()
	} else {
		transportCredentials = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.DialContext(ctx, cfg.GRPCAddress, grpc.WithTransportCredentials(transportCredentials), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("connect gRPC endpoint: %w", err)
	}

	encoding := newEncodingConfig()
	manager := &WalletManager{
		config:   cfg,
		grpcConn: conn,
		codec:    encoding.codec,
		registry: encoding.registry,
		auth:     authtypes.NewQueryClient(conn),
		tx:       cosmostx.NewServiceClient(conn),
		queues:   make(map[*Wallet]*walletQueue),
	}
	manager.client = client.Context{}.
		WithChainID(cfg.ChainID).
		WithCodec(encoding.codec).
		WithInterfaceRegistry(encoding.registry).
		WithTxConfig(encoding.txConfig).
		WithLegacyAmino(encoding.amino).
		WithGRPCClient(conn).
		WithBroadcastMode("sync").
		WithSkipConfirmation(true).
		WithSignModeStr("direct")
	return manager, nil
}

// Close stops queued submissions, fails work that has not started, and
// releases the gRPC connection owned by the manager.
func (m *WalletManager) Close() error {
	if m == nil {
		return nil
	}
	m.queuesMu.Lock()
	if m.closed {
		m.queuesMu.Unlock()
		return nil
	}
	m.closed = true
	queues := make([]*walletQueue, 0, len(m.queues))
	for _, queue := range m.queues {
		queues = append(queues, queue)
	}
	m.queuesMu.Unlock()
	for _, queue := range queues {
		queue.stop()
	}
	if m.grpcConn != nil {
		return m.grpcConn.Close()
	}
	return nil
}

// BroadcastTx queues a transaction behind earlier work for wallet and waits
// for CheckTx. Call WaitForTx to wait for block inclusion.
func (m *WalletManager) BroadcastTx(ctx context.Context, wallet *Wallet, messages ...sdk.Msg) (*TxResult, error) {
	return m.BroadcastTxWithOptions(ctx, wallet, BroadcastOptions{}, messages...)
}

// BroadcastTxWithOptions is BroadcastTx with transaction-specific fee, gas,
// and memo settings. It uses the same FIFO queue as BroadcastTxAsync.
func (m *WalletManager) BroadcastTxWithOptions(ctx context.Context, wallet *Wallet, options BroadcastOptions, messages ...sdk.Msg) (*TxResult, error) {
	future, err := m.BroadcastTxAsyncWithOptions(ctx, wallet, options, messages...)
	if err != nil {
		return nil, err
	}
	return future.Result(ctx)
}

// broadcastTxNow executes one queue-owned transaction without re-enqueueing.
func (m *WalletManager) broadcastTxNow(ctx context.Context, wallet *Wallet, options BroadcastOptions, messages ...sdk.Msg) (*TxResult, error) {
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	if err := validateBroadcastInput(wallet, messages); err != nil {
		return nil, err
	}

	wallet.txMu.Lock()
	defer wallet.txMu.Unlock()

	result, err := m.signAndBroadcast(ctx, wallet, options, messages...)
	if !isSequenceMismatch(result, err) {
		return result, err
	}
	return m.signAndBroadcast(ctx, wallet, options, messages...)
}

// queueForWallet returns the single FIFO worker associated with wallet.
func (m *WalletManager) queueForWallet(wallet *Wallet) (*walletQueue, error) {
	m.queuesMu.Lock()
	defer m.queuesMu.Unlock()
	if m.closed {
		return nil, ErrWalletManagerClosed
	}
	if m.queues == nil {
		m.queues = make(map[*Wallet]*walletQueue)
	}
	if queue := m.queues[wallet]; queue != nil {
		return queue, nil
	}
	queue := newWalletQueue(m, wallet, m.config.QueueCapacity)
	m.queues[wallet] = queue
	return queue, nil
}

// validateBroadcastInput verifies data shared by synchronous and queued calls.
func validateBroadcastInput(wallet *Wallet, messages []sdk.Msg) error {
	if wallet == nil {
		return fmt.Errorf("wallet is required")
	}
	if len(messages) == 0 {
		return fmt.Errorf("at least one message is required")
	}
	for i, message := range messages {
		if message == nil {
			return fmt.Errorf("message %d is nil", i)
		}
	}
	return nil
}

// signAndBroadcast builds, signs, and submits one already-serialized request.
func (m *WalletManager) signAndBroadcast(ctx context.Context, wallet *Wallet, options BroadcastOptions, messages ...sdk.Msg) (*TxResult, error) {
	accountNumber, sequence, err := m.accountSequence(ctx, wallet.Address())
	if err != nil {
		return nil, err
	}
	gasPrices := options.GasPrices
	if gasPrices == "" {
		gasPrices = m.config.GasPrices
	}
	if _, err := sdk.ParseDecCoins(gasPrices); err != nil {
		return nil, fmt.Errorf("invalid gas prices %q: %w", gasPrices, err)
	}

	clientContext := m.client.WithKeyring(wallet.keyring).WithFromName(wallet.keyName).WithFromAddress(wallet.address)
	txf := clienttx.Factory{}.
		WithTxConfig(clientContext.TxConfig).
		WithAccountRetriever(clientContext.AccountRetriever).
		WithChainID(m.config.ChainID).
		WithGasPrices(gasPrices).
		WithGasAdjustment(m.config.GasAdjustment).
		WithKeybase(wallet.keyring).
		WithAccountNumber(accountNumber).
		WithSequence(sequence).
		WithSignMode(signing.SignMode_SIGN_MODE_DIRECT).
		WithFromName(wallet.keyName).
		WithMemo(options.Memo)

	gasLimit := options.GasLimit
	if gasLimit == 0 {
		gasLimit = m.config.GasLimit
	}
	if gasLimit > 0 {
		txf = txf.WithGas(gasLimit)
	} else {
		_, gas, err := clienttx.CalculateGas(clientContext, txf.WithGas(250000), messages...)
		if err != nil {
			return nil, fmt.Errorf("simulate transaction gas: %w", err)
		}
		txf = txf.WithGas(gas)
	}

	txBuilder, err := txf.BuildUnsignedTx(messages...)
	if err != nil {
		return nil, fmt.Errorf("build transaction: %w", err)
	}
	if err := clienttx.Sign(ctx, txf, wallet.keyName, txBuilder, true); err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}
	txBytes, err := clientContext.TxConfig.TxEncoder()(txBuilder.GetTx())
	if err != nil {
		return nil, fmt.Errorf("encode transaction: %w", err)
	}
	return m.broadcastTxBytes(ctx, txBytes)
}

// accountSequence reads the current account number and sequence from Atlas.
func (m *WalletManager) accountSequence(ctx context.Context, address string) (uint64, uint64, error) {
	response, err := m.auth.Account(ctx, &authtypes.QueryAccountRequest{Address: address})
	if err != nil {
		return 0, 0, fmt.Errorf("query account %s: %w", address, err)
	}
	var account sdk.AccountI
	if err := m.registry.UnpackAny(response.Account, &account); err != nil {
		return 0, 0, fmt.Errorf("unpack account %s: %w", address, err)
	}
	return account.GetAccountNumber(), account.GetSequence(), nil
}

// broadcastTxBytes submits encoded transaction bytes through the Cosmos API.
func (m *WalletManager) broadcastTxBytes(ctx context.Context, txBytes []byte) (*TxResult, error) {
	response, err := m.tx.BroadcastTx(ctx, &cosmostx.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    cosmostx.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return nil, fmt.Errorf("broadcast transaction: %w", err)
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

// WaitForTx waits until the transaction is committed. Its lifetime is wholly
// controlled by ctx; callers should use context.WithTimeout when appropriate.
func (m *WalletManager) WaitForTx(ctx context.Context, hash string) (*TxResult, error) {
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	if strings.TrimSpace(hash) == "" {
		return nil, fmt.Errorf("transaction hash is required")
	}
	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()
	for {
		response, err := m.tx.GetTx(ctx, &cosmostx.GetTxRequest{Hash: hash})
		if err == nil && response.TxResponse != nil {
			result := &TxResult{Hash: response.TxResponse.TxHash, Height: response.TxResponse.Height, Code: response.TxResponse.Code, Log: response.TxResponse.RawLog}
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

// isSequenceMismatch identifies the CheckTx rejection that is safe to retry.
func isSequenceMismatch(result *TxResult, err error) bool {
	message := ""
	if result != nil {
		message += result.Log
	}
	if err != nil {
		message += " " + err.Error()
	}
	message = strings.ToLower(message)
	return strings.Contains(message, "account sequence mismatch") ||
		strings.Contains(message, "incorrect account sequence")
}

type encodingConfig struct {
	codec    codec.Codec
	registry types.InterfaceRegistry
	txConfig client.TxConfig
	amino    *codec.LegacyAmino
}

// newEncodingConfig registers the Cosmos and Atlas interfaces used in a tx.
func newEncodingConfig() encodingConfig {
	amino := codec.NewLegacyAmino()
	registry := types.NewInterfaceRegistry()
	marshaler := codec.NewProtoCodec(registry)
	std.RegisterLegacyAminoCodec(amino)
	std.RegisterInterfaces(registry)
	storagetypes.RegisterInterfaces(registry)
	filetreetypes.RegisterInterfaces(registry)
	registry.RegisterImplementations(
		(*sdk.AccountI)(nil),
		&authtypes.BaseAccount{},
		&vestingtypes.BaseVestingAccount{},
		&vestingtypes.ContinuousVestingAccount{},
		&vestingtypes.DelayedVestingAccount{},
		&vestingtypes.PeriodicVestingAccount{},
		&vestingtypes.PermanentLockedAccount{},
	)
	return encodingConfig{
		codec:    marshaler,
		registry: registry,
		txConfig: authtx.NewTxConfig(marshaler, []signing.SignMode{signing.SignMode_SIGN_MODE_DIRECT}),
		amino:    amino,
	}
}
