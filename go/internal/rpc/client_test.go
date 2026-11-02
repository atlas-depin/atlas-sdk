package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBroadcastTxSync(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"code":0,"log":"","hash":"ABC123"}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	result, err := client.BroadcastTxSync(context.Background(), []byte("signed transaction"))
	if err != nil {
		t.Fatalf("BroadcastTxSync() error = %v", err)
	}
	if result.Hash != "ABC123" || result.Code != 0 {
		t.Fatalf("unexpected result: %#v", result)
	}
}
