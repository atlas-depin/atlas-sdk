package atlas

import (
	"io"
	"net/http"
	"time"
)

// Transport selects how transactions are broadcast. Typed Atlas module queries
// currently require gRPC.
type Transport string

const (
	// TransportAuto prefers gRPC and uses RPC when gRPC is not selected for
	// transaction broadcasting.
	TransportAuto Transport = "auto"
	// TransportGRPC broadcasts through the Cosmos Tx gRPC service.
	TransportGRPC Transport = "grpc"
	// TransportRPC broadcasts through CometBFT JSON-RPC.
	TransportRPC Transport = "rpc"
)

// Config configures a Client.
type Config struct {
	ChainID     string
	GRPCAddress string
	RPCAddress  string
	Transport   Transport

	// GRPCInsecure disables TLS for gRPC. It is suitable for local development
	// only; production clients should use TLS.
	GRPCInsecure bool

	// GasPrices is a Cosmos decimal coin string, such as "0.025uatl".
	GasPrices     string
	GasAdjustment float64
	// GasLimit avoids gRPC simulation when non-zero. It is required for signed
	// transactions if no gRPC endpoint is configured.
	GasLimit uint64

	HTTPClient *http.Client
}

// PageRequest is cursor pagination for list operations.
type PageRequest struct {
	Key        []byte
	Offset     uint64
	Limit      uint64
	CountTotal bool
	Reverse    bool
}

// PageResponse describes the next page of a list operation.
type PageResponse struct {
	NextKey []byte
	Total   uint64
}

// StorageStats is the network-wide storage state reported by the chain.
type StorageStats struct {
	SubscriptionCount int32
	SpacePurchased    int64
	SpaceUsed         int64
	SpaceReplicas     int64
	ProviderCount     int32
	SpaceAvailable    int64
}

// Subscription is an account storage allocation.
type Subscription struct {
	ID               string
	Start            time.Time
	End              time.Time
	Status           string
	SpaceAvailable   int64
	SpaceUsed        int64
	ReplicaSpaceUsed int64
	Credits          string
}

// AccountStorageStats is derived from all of an account's subscriptions and
// files. It is distinct from the network-wide StorageStats endpoint.
type AccountStorageStats struct {
	SubscriptionCount int
	SpaceAvailable    int64
	SpaceUsed         int64
	ReplicaSpaceUsed  int64
	FileCount         int
	FileBytes         int64
}

// File is a storage-file record indexed by the chain.
type File struct {
	ID                string
	MerkleRoot        string
	Creator           string
	SubscriptionID    string
	Status            string
	Size              int64
	Replicas          int32
	Providers         []string
	UnstableProviders []string
	Start             int64
}

// Provider is a registered Atlas storage provider.
type Provider struct {
	Address        string
	Hostname       string
	CreatedAt      int64
	CreditDelta    int64
	SpaceAvailable int64
	SpaceUsed      int64
}

// FileTreeNode is a node stored in the Atlas filetree module.
type FileTreeNode struct {
	NodeType string
	Contents string
	Viewers  string
	Editors  string
}

// TxResult is a normalized result from either supported broadcast transport.
type TxResult struct {
	Hash   string
	Height int64
	Code   uint32
	Log    string
}

// WalletConfig configures a Cosmos SDK keyring wallet.
type WalletConfig struct {
	KeyName string
	Backend string
	HomeDir string
	Input   io.Reader
}

// BuyStorageRequest describes a storage purchase.
type BuyStorageRequest struct {
	Duration  int64
	Bytes     int64
	IsDefault bool
}

// UploadRequest describes the on-chain and provider upload workflow.
// Source must be seekable because it is first committed and then streamed to a
// provider. A positive Size is verified against the source while calculating
// the root; zero leaves size discovery to the SDK.
type UploadRequest struct {
	FID            string
	Source         io.ReadSeeker
	Size           int64
	Replicas       int32
	SubscriptionID string

	// MinSuccessfulUploads defaults to one. Atlas can heal partial replication
	// after a provider's initial proof succeeds.
	MinSuccessfulUploads int
	ProviderTimeout      time.Duration

	// FileTree, when supplied, is posted with MsgPostFile in the same
	// transaction before any provider receives file bytes.
	FileTree *FileTreePostRequest
}

// FileTreePostRequest describes an optional filetree node created alongside a
// storage-file record in a combined upload transaction.
type FileTreePostRequest struct {
	Path     string
	NodeType string
	Contents string
	Viewers  string
	Editors  string
}

// UploadResult reports the on-chain transaction and providers that accepted
// the file bytes.
type UploadResult struct {
	Transaction *TxResult
	MerkleRoot  []byte
	Size        int64
	Providers   []string
}

// DownloadResult describes a verified provider download.
type DownloadResult struct {
	Provider string
	Size     int64
	Verified bool
}
