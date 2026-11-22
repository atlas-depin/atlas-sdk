// Package wallet signs and broadcasts Atlas Cosmos transactions.
package wallet

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// BroadcastTxAsync queues a transaction and immediately returns a future. The
// wallet's queue processes every synchronous and asynchronous call in FIFO
// order.
func (m *WalletManager) BroadcastTxAsync(ctx context.Context, wallet *Wallet, messages ...sdk.Msg) (*TxFuture, error) {
	return m.BroadcastTxAsyncWithOptions(ctx, wallet, BroadcastOptions{}, messages...)
}

// BroadcastTxAsyncWithOptions queues a transaction with transaction-specific
// settings. The context controls queueing and broadcast. Do not mutate message
// values after calling it.
func (m *WalletManager) BroadcastTxAsyncWithOptions(ctx context.Context, wallet *Wallet, options BroadcastOptions, messages ...sdk.Msg) (*TxFuture, error) {
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	if err := validateBroadcastInput(wallet, messages); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	queue, err := m.queueForWallet(wallet)
	if err != nil {
		return nil, err
	}
	future := newTxFuture(m)
	request := &queuedTx{
		ctx:      ctx,
		options:  options,
		messages: append([]sdk.Msg(nil), messages...),
		future:   future,
	}
	if err := queue.submit(ctx, request); err != nil {
		return nil, err
	}
	return future, nil
}

// Result waits for the queued transaction's CheckTx result. A context timeout
// here does not cancel the queued transaction; its submission context does.
func (f *TxFuture) Result(ctx context.Context) (*TxResult, error) {
	if f == nil {
		return nil, fmt.Errorf("transaction future is required")
	}
	select {
	case <-f.done:
		f.resultMu.RLock()
		defer f.resultMu.RUnlock()
		return f.result, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Wait waits for CheckTx, then waits for the accepted transaction to be
// committed. Its context controls only this wait, not submission itself.
func (f *TxFuture) Wait(ctx context.Context) (*TxResult, error) {
	result, err := f.Result(ctx)
	if err != nil || result == nil {
		return result, err
	}
	if f.manager == nil {
		return nil, fmt.Errorf("transaction future has no wallet manager")
	}
	return f.manager.WaitForTx(ctx, result.Hash)
}

// newTxFuture creates an incomplete future associated with a manager.
func newTxFuture(manager *WalletManager) *TxFuture {
	return &TxFuture{manager: manager, done: make(chan struct{})}
}

// complete stores a transaction outcome and wakes every Result waiter.
func (f *TxFuture) complete(result *TxResult, err error) {
	f.resultOnce.Do(func() {
		f.resultMu.Lock()
		f.result = result
		f.err = err
		f.resultMu.Unlock()
		close(f.done)
	})
}

// queuedTx is the immutable work item consumed by one wallet queue.
type queuedTx struct {
	ctx      context.Context
	options  BroadcastOptions
	messages []sdk.Msg
	future   *TxFuture
}

// walletQueue is a FIFO worker for one account. Queue state is protected
// separately so manager shutdown cannot strand a future during enqueue.
type walletQueue struct {
	manager *WalletManager
	wallet  *Wallet

	capacity       int
	pending        []*queuedTx
	workAvailable  chan struct{}
	spaceAvailable chan struct{}
	stopCh         chan struct{}
	done           chan struct{}

	mu      sync.Mutex
	stopped bool
	execute func(*queuedTx)
}

// newWalletQueue creates and starts a per-wallet transaction worker.
func newWalletQueue(manager *WalletManager, wallet *Wallet, capacity int) *walletQueue {
	queue := &walletQueue{
		manager:        manager,
		wallet:         wallet,
		capacity:       capacity,
		workAvailable:  make(chan struct{}, 1),
		spaceAvailable: make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
	queue.execute = func(request *queuedTx) {
		result, err := manager.broadcastTxNow(request.ctx, wallet, request.options, request.messages...)
		request.future.complete(result, err)
	}
	go queue.run()
	return queue
}

// submit enqueues work unless shutdown has started or ctx expires.
func (q *walletQueue) submit(ctx context.Context, request *queuedTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mu.Lock()
		if q.stopped {
			q.mu.Unlock()
			return ErrWalletManagerClosed
		}
		if q.capacity == -1 || len(q.pending) < q.capacity {
			q.pending = append(q.pending, request)
			q.mu.Unlock()
			q.signalWork()
			return nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.stopCh:
			return ErrWalletManagerClosed
		case <-q.spaceAvailable:
		}
	}
}

// stop prevents new submissions and fails work that has not started.
func (q *walletQueue) stop() {
	q.mu.Lock()
	pending := q.pending
	q.pending = nil
	if !q.stopped {
		q.stopped = true
		close(q.stopCh)
	}
	q.mu.Unlock()
	for _, request := range pending {
		request.future.complete(nil, ErrWalletManagerClosed)
	}
}

// run executes one request at a time until the queue is stopped.
func (q *walletQueue) run() {
	defer close(q.done)
	for {
		request, stopped := q.next()
		if stopped {
			return
		}
		if request != nil {
			q.execute(request)
			continue
		}
		select {
		case <-q.stopCh:
			return
		case <-q.workAvailable:
		}
	}
}

// next removes one FIFO item and reports whether shutdown has started.
func (q *walletQueue) next() (*queuedTx, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return nil, true
	}
	if len(q.pending) == 0 {
		return nil, false
	}
	request := q.pending[0]
	q.pending[0] = nil
	q.pending = q.pending[1:]
	if q.capacity != -1 {
		q.signalSpace()
	}
	return request, false
}

// signalWork wakes the worker after a newly queued transaction.
func (q *walletQueue) signalWork() {
	select {
	case q.workAvailable <- struct{}{}:
	default:
	}
}

// signalSpace wakes one broadcast waiting for bounded queue capacity.
func (q *walletQueue) signalSpace() {
	select {
	case q.spaceAvailable <- struct{}{}:
	default:
	}
}
