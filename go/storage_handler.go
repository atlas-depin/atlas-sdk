package atlas

import (
	"context"
	"fmt"
)

// StorageHandler owns composed storage workflows. It is intentionally the
// only high-level service on Client: queries, generic transactions, wallet
// management, and provider I/O remain separate concerns as the SDK grows.
type StorageHandler struct {
	client *Client
}

// BuyStorage purchases a storage subscription through the owning client.
func (h *StorageHandler) BuyStorage(ctx context.Context, wallet *Wallet, request BuyStorageRequest) (*TxResult, error) {
	if h == nil || h.client == nil {
		return nil, fmt.Errorf("storage handler is not initialized")
	}
	return h.client.BuyStorage(ctx, wallet, request)
}
