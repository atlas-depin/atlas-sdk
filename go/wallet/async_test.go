package wallet

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestWalletQueueProcessesRequestsInFIFOOrder verifies per-wallet ordering.
func TestWalletQueueProcessesRequestsInFIFOOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	queue := newTestWalletQueue(2, func(request *queuedTx) {
		mu.Lock()
		order = append(order, request.options.Memo)
		mu.Unlock()
		request.future.complete(&TxResult{Hash: request.options.Memo}, nil)
	})
	defer stopTestWalletQueue(t, queue)

	first := newTxFuture(nil)
	second := newTxFuture(nil)
	if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), options: BroadcastOptions{Memo: "first"}, future: first}); err != nil {
		t.Fatalf("first submit error = %v", err)
	}
	if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), options: BroadcastOptions{Memo: "second"}, future: second}); err != nil {
		t.Fatalf("second submit error = %v", err)
	}
	if _, err := first.Result(t.Context()); err != nil {
		t.Fatalf("first Result() error = %v", err)
	}
	if _, err := second.Result(t.Context()); err != nil {
		t.Fatalf("second Result() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := len(order), 2; got != want {
		t.Fatalf("completed %d requests, want %d", got, want)
	}
	if order[0] != "first" || order[1] != "second" {
		t.Fatalf("order = %v, want [first second]", order)
	}
}

// TestWalletQueueStopFailsRequestsThatHaveNotStarted verifies shutdown safety.
func TestWalletQueueStopFailsRequestsThatHaveNotStarted(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	queue := newTestWalletQueue(2, func(request *queuedTx) {
		close(started)
		<-release
		request.future.complete(&TxResult{Hash: "started"}, nil)
	})

	first := newTxFuture(nil)
	second := newTxFuture(nil)
	if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), future: first}); err != nil {
		t.Fatalf("first submit error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), future: second}); err != nil {
		t.Fatalf("second submit error = %v", err)
	}
	queue.stop()
	close(release)

	if _, err := first.Result(t.Context()); err != nil {
		t.Fatalf("first Result() error = %v", err)
	}
	if _, err := second.Result(t.Context()); err != ErrWalletManagerClosed {
		t.Fatalf("second Result() error = %v, want %v", err, ErrWalletManagerClosed)
	}
	select {
	case <-queue.done:
	case <-time.After(time.Second):
		t.Fatal("queue did not stop")
	}
}

// TestWalletQueueAcceptsUnlimitedPendingRequests verifies the -1 mode.
func TestWalletQueueAcceptsUnlimitedPendingRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	queue := newTestWalletQueue(-1, func(request *queuedTx) {
		close(started)
		<-release
		request.future.complete(&TxResult{}, nil)
	})

	first := newTxFuture(nil)
	if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), future: first}); err != nil {
		t.Fatalf("first submit error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	for i := 0; i < 100; i++ {
		if err := queue.submit(t.Context(), &queuedTx{ctx: t.Context(), future: newTxFuture(nil)}); err != nil {
			t.Fatalf("submit %d error = %v", i, err)
		}
	}
	queue.stop()
	close(release)
	if _, err := first.Result(t.Context()); err != nil {
		t.Fatalf("first Result() error = %v", err)
	}
	select {
	case <-queue.done:
	case <-time.After(time.Second):
		t.Fatal("queue did not stop")
	}
}

// TestTxFutureResultTimeoutDoesNotConsumeTheResult verifies repeatable waits.
func TestTxFutureResultTimeoutDoesNotConsumeTheResult(t *testing.T) {
	future := newTxFuture(nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := future.Result(ctx); err == nil {
		t.Fatal("Result() error = nil, want canceled context")
	}
	future.complete(&TxResult{Hash: "ABC"}, nil)
	result, err := future.Result(t.Context())
	if err != nil {
		t.Fatalf("second Result() error = %v", err)
	}
	if result.Hash != "ABC" {
		t.Fatalf("result hash = %q, want ABC", result.Hash)
	}
}

// newTestWalletQueue creates a queue whose execution is controlled by a test.
func newTestWalletQueue(capacity int, execute func(*queuedTx)) *walletQueue {
	queue := &walletQueue{
		capacity:       capacity,
		workAvailable:  make(chan struct{}, 1),
		spaceAvailable: make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
		execute:        execute,
	}
	go queue.run()
	return queue
}

// stopTestWalletQueue stops a test queue and reports a leaked worker.
func stopTestWalletQueue(t *testing.T, queue *walletQueue) {
	t.Helper()
	queue.stop()
	select {
	case <-queue.done:
	case <-time.After(time.Second):
		t.Fatal("queue did not stop")
	}
}
