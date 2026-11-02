package atlas

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"github.com/atlas-depin/atlas-sdk/go/internal/rpc"
	filetreetypes "github.com/atlas-depin/atlas/x/filetree/types"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	"github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	vestingtypes "github.com/cosmos/cosmos-sdk/x/auth/vesting/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client provides access to Atlas chain queries, transactions, and provider
// transfers. It is safe to use for concurrent queries; signing is serialized
// to prevent account-sequence races.
type Client struct {
	config Config

	grpcConn *grpc.ClientConn
	rpc      *rpc.Client

	clientContext client.Context
	codec         codec.Codec
	registry      types.InterfaceRegistry

	auth     authtypes.QueryClient
	storage  storagetypes.QueryClient
	filetree filetreetypes.QueryClient
	txMu     sync.Mutex

	// Storage coordinates high-level storage workflows that span modules and
	// provider I/O. Low-level operations remain available on Client.
	Storage *StorageHandler
}

// NewClient creates a connected Atlas client. Auto and gRPC mode require a
// gRPC endpoint because Atlas module queries are published as gRPC services.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	cfg.Transport = normalizeTransport(cfg.Transport)
	if cfg.ChainID == "" {
		return nil, fmt.Errorf("chain ID is required")
	}
	if cfg.Transport != TransportAuto && cfg.Transport != TransportGRPC && cfg.Transport != TransportRPC {
		return nil, fmt.Errorf("unsupported transport %q", cfg.Transport)
	}
	if cfg.GasAdjustment == 0 {
		cfg.GasAdjustment = 1.2
	}
	if cfg.GasPrices == "" {
		cfg.GasPrices = "0.025uatl"
	}
	if _, err := sdk.ParseDecCoins(cfg.GasPrices); err != nil {
		return nil, fmt.Errorf("invalid gas prices %q: %w", cfg.GasPrices, err)
	}

	encoding := newEncodingConfig()
	c := &Client{
		config:   cfg,
		codec:    encoding.codec,
		registry: encoding.registry,
		clientContext: client.Context{}.
			WithChainID(cfg.ChainID).
			WithCodec(encoding.codec).
			WithInterfaceRegistry(encoding.registry).
			WithTxConfig(encoding.txConfig).
			WithLegacyAmino(encoding.amino).
			WithBroadcastMode("sync").
			WithSkipConfirmation(true).
			WithSignModeStr("direct"),
	}
	c.Storage = &StorageHandler{client: c}

	if cfg.GRPCAddress != "" {
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
		c.grpcConn = conn
		c.clientContext = c.clientContext.WithGRPCClient(conn)
		c.auth = authtypes.NewQueryClient(conn)
		c.storage = storagetypes.NewQueryClient(conn)
		c.filetree = filetreetypes.NewQueryClient(conn)
	}
	if cfg.RPCAddress != "" {
		rpcClient, err := rpc.NewClient(cfg.RPCAddress, cfg.HTTPClient)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		c.rpc = rpcClient
	}
	if cfg.Transport == TransportAuto || cfg.Transport == TransportGRPC {
		if c.grpcConn == nil {
			return nil, c.closeWithError(fmt.Errorf("gRPC address is required for %s mode", cfg.Transport))
		}
	}
	if cfg.Transport == TransportRPC && c.rpc == nil {
		return nil, c.closeWithError(fmt.Errorf("RPC address is required for RPC mode"))
	}
	return c, nil
}

func normalizeTransport(t Transport) Transport {
	if t == "" {
		return TransportAuto
	}
	return Transport(strings.ToLower(string(t)))
}

func (c *Client) closeWithError(err error) error {
	_ = c.Close()
	return err
}

// Close releases the gRPC connection owned by the client.
func (c *Client) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}

func (c *Client) requireGRPC(operation string) error {
	if c.grpcConn == nil {
		return fmt.Errorf("%s requires a gRPC endpoint; Atlas typed module queries are not exposed through CometBFT JSON-RPC", operation)
	}
	return nil
}

type encodingConfig struct {
	codec    codec.Codec
	registry types.InterfaceRegistry
	txConfig client.TxConfig
	amino    *codec.LegacyAmino
}

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
		txConfig: tx.NewTxConfig(marshaler, []signing.SignMode{signing.SignMode_SIGN_MODE_DIRECT}),
		amino:    amino,
	}
}
