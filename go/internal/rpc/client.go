// Package rpc implements the small CometBFT JSON-RPC surface used by the SDK.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Client is a CometBFT JSON-RPC client.
type Client struct {
	endpoint string
	http     *http.Client
}

// NewClient creates a JSON-RPC client for endpoint.
func NewClient(endpoint string, httpClient *http.Client) (*Client, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("RPC endpoint is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{endpoint: endpoint, http: httpClient}, nil
}

// TxResult is the part of a CometBFT transaction response useful to callers.
type TxResult struct {
	Hash   string
	Height int64
	Code   uint32
	Log    string
}

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

func (e *rpcError) Error() string {
	if e.Data == "" {
		return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("RPC error %d: %s: %s", e.Code, e.Message, e.Data)
}

func (c *Client) call(ctx context.Context, method string, params interface{}, result interface{}) error {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("encode RPC request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build RPC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform RPC request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("RPC endpoint returned %s", resp.Status)
	}

	var envelope rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("decode RPC response: %w", err)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode RPC result: %w", err)
	}
	return nil
}

// BroadcastTxSync submits an already signed transaction to the node mempool.
func (c *Client) BroadcastTxSync(ctx context.Context, txBytes []byte) (*TxResult, error) {
	var result struct {
		Code uint32 `json:"code"`
		Log  string `json:"log"`
		Hash string `json:"hash"`
	}
	if err := c.call(ctx, "broadcast_tx_sync", struct {
		Tx []byte `json:"tx"`
	}{Tx: txBytes}, &result); err != nil {
		return nil, err
	}
	return &TxResult{Hash: result.Hash, Code: result.Code, Log: result.Log}, nil
}

// Tx gets a committed transaction by hash.
func (c *Client) Tx(ctx context.Context, hash string) (*TxResult, error) {
	if !strings.HasPrefix(hash, "0x") {
		hash = "0x" + hash
	}
	var result struct {
		Hash   string `json:"hash"`
		Height int64  `json:"height,string"`
		Result struct {
			Code uint32 `json:"code"`
			Log  string `json:"log"`
		} `json:"tx_result"`
	}
	if err := c.call(ctx, "tx", struct {
		Hash  string `json:"hash"`
		Prove bool   `json:"prove"`
	}{Hash: hash}, &result); err != nil {
		return nil, err
	}
	return &TxResult{Hash: result.Hash, Height: result.Height, Code: result.Result.Code, Log: result.Result.Log}, nil
}
