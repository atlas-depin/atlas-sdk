package atlas

import (
	"context"
	"fmt"

	filetreetypes "github.com/atlas-depin/atlas/x/filetree/types"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	querytypes "github.com/cosmos/cosmos-sdk/types/query"
)

// StorageStats returns the network-wide storage state.
func (c *Client) StorageStats(ctx context.Context) (*StorageStats, error) {
	if err := c.requireGRPC("StorageStats"); err != nil {
		return nil, err
	}
	response, err := c.storage.StorageStats(ctx, &storagetypes.QueryStorageStatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("query storage stats: %w", err)
	}
	return &StorageStats{
		SubscriptionCount: response.SubscriptionCount,
		SpacePurchased:    response.SpacePurchased,
		SpaceUsed:         response.SpaceUsed,
		SpaceReplicas:     response.SpaceReplicas,
		ProviderCount:     response.ProviderCount,
		SpaceAvailable:    response.SpaceAvailable,
	}, nil
}

// AccountStorageStats derives usage from every subscription and file owned by
// address. It follows cursor pagination until the account's records are read.
func (c *Client) AccountStorageStats(ctx context.Context, address string) (*AccountStorageStats, error) {
	if address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	stats := &AccountStorageStats{}
	var page PageRequest
	for {
		subscriptions, next, err := c.Subscriptions(ctx, address, page)
		if err != nil {
			return nil, err
		}
		for _, subscription := range subscriptions {
			stats.SubscriptionCount++
			stats.SpaceAvailable += subscription.SpaceAvailable
			stats.SpaceUsed += subscription.SpaceUsed
			stats.ReplicaSpaceUsed += subscription.ReplicaSpaceUsed
		}
		if len(next.NextKey) == 0 {
			break
		}
		page = PageRequest{Key: next.NextKey}
	}

	page = PageRequest{}
	for {
		files, next, err := c.Files(ctx, address, "", page)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			stats.FileCount++
			stats.FileBytes += file.Size
		}
		if len(next.NextKey) == 0 {
			break
		}
		page = PageRequest{Key: next.NextKey}
	}
	return stats, nil
}

// Subscriptions lists storage subscriptions belonging to address.
func (c *Client) Subscriptions(ctx context.Context, address string, page PageRequest) ([]Subscription, PageResponse, error) {
	if err := c.requireGRPC("Subscriptions"); err != nil {
		return nil, PageResponse{}, err
	}
	if address == "" {
		return nil, PageResponse{}, fmt.Errorf("account address is required")
	}
	response, err := c.storage.Subscriptions(ctx, &storagetypes.QuerySubscriptionsRequest{
		SubscriberAddress: address,
		Pagination:        toSDKPage(page),
	})
	if err != nil {
		return nil, PageResponse{}, fmt.Errorf("query subscriptions: %w", err)
	}
	items := make([]Subscription, 0, len(response.Subscriptions))
	for _, subscription := range response.Subscriptions {
		items = append(items, Subscription{
			ID:               subscription.Id,
			Start:            subscription.Start,
			End:              subscription.End,
			Status:           subscription.Status,
			SpaceAvailable:   subscription.SpaceAvailable,
			SpaceUsed:        subscription.SpaceUsed,
			ReplicaSpaceUsed: subscription.ReplicaSpaceUsed,
			Credits:          subscription.Credits.String(),
		})
	}
	return items, fromSDKPage(response.Pagination), nil
}

// Files lists storage-module files. subscriptionID can be empty to include
// every file owned by address.
func (c *Client) Files(ctx context.Context, address, subscriptionID string, page PageRequest) ([]File, PageResponse, error) {
	if err := c.requireGRPC("Files"); err != nil {
		return nil, PageResponse{}, err
	}
	if address == "" {
		return nil, PageResponse{}, fmt.Errorf("account address is required")
	}
	response, err := c.storage.Files(ctx, &storagetypes.QueryFilesRequest{
		Creator:      address,
		Subscription: subscriptionID,
		Pagination:   toSDKPage(page),
	})
	if err != nil {
		return nil, PageResponse{}, fmt.Errorf("query files: %w", err)
	}
	items := make([]File, 0, len(response.Files))
	for _, file := range response.Files {
		items = append(items, fileFromProto(file))
	}
	return items, fromSDKPage(response.Pagination), nil
}

// File returns a storage file by its ID.
func (c *Client) File(ctx context.Context, fid string) (*File, error) {
	if err := c.requireGRPC("File"); err != nil {
		return nil, err
	}
	if fid == "" {
		return nil, fmt.Errorf("file ID is required")
	}
	response, err := c.storage.File(ctx, &storagetypes.QueryFileRequest{Fid: fid})
	if err != nil {
		return nil, fmt.Errorf("query file: %w", err)
	}
	if response.File == nil {
		return nil, fmt.Errorf("file %q was not returned by the chain", fid)
	}
	file := fileFromProto(response.File)
	return &file, nil
}

// Providers returns the registered storage providers.
func (c *Client) Providers(ctx context.Context) ([]Provider, error) {
	if err := c.requireGRPC("Providers"); err != nil {
		return nil, err
	}
	response, err := c.storage.Providers(ctx, &storagetypes.QueryProvidersRequest{})
	if err != nil {
		return nil, fmt.Errorf("query providers: %w", err)
	}
	providers := make([]Provider, 0, len(response.Providers))
	for _, provider := range response.Providers {
		providers = append(providers, Provider{
			Address:        provider.Address,
			Hostname:       provider.Hostname,
			CreatedAt:      provider.CreatedAt,
			CreditDelta:    provider.CreditDelta,
			SpaceAvailable: provider.SpaceAvailable,
			SpaceUsed:      provider.SpaceUsed,
		})
	}
	return providers, nil
}

// FileTreePaths returns every path below basePath for owner.
func (c *Client) FileTreePaths(ctx context.Context, owner, basePath string) ([]string, error) {
	if err := c.requireGRPC("FileTreePaths"); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("owner address is required")
	}
	response, err := c.filetree.FileTreePaths(ctx, &filetreetypes.QueryFileTreePathsRequest{
		Owner: owner, Basepath: basePath,
	})
	if err != nil {
		return nil, fmt.Errorf("query file tree paths: %w", err)
	}
	return append([]string(nil), response.Paths...), nil
}

// FileTreeNode returns an owner's filetree node at path.
func (c *Client) FileTreeNode(ctx context.Context, owner, path string) (*FileTreeNode, error) {
	if err := c.requireGRPC("FileTreeNode"); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("owner address is required")
	}
	response, err := c.filetree.TreeNode(ctx, &filetreetypes.QueryTreeNodeRequest{Owner: owner, Path: path})
	if err != nil {
		return nil, fmt.Errorf("query file tree node: %w", err)
	}
	if response.Node == nil {
		return nil, fmt.Errorf("filetree node %q was not returned by the chain", path)
	}
	return treeNodeFromProto(response.Node), nil
}

// FileTreeChildren returns the direct children of an owner's filetree path.
func (c *Client) FileTreeChildren(ctx context.Context, owner, path string) ([]FileTreeNode, error) {
	if err := c.requireGRPC("FileTreeChildren"); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("owner address is required")
	}
	response, err := c.filetree.TreeNodeChildren(ctx, &filetreetypes.QueryTreeNodeChildrenRequest{Owner: owner, Path: path})
	if err != nil {
		return nil, fmt.Errorf("query filetree children: %w", err)
	}
	items := make([]FileTreeNode, 0, len(response.Nodes))
	for _, node := range response.Nodes {
		if node != nil {
			items = append(items, *treeNodeFromProto(node))
		}
	}
	return items, nil
}

func toSDKPage(page PageRequest) *querytypes.PageRequest {
	if page.Limit == 0 {
		page.Limit = 100
	}
	return &querytypes.PageRequest{
		Key:        append([]byte(nil), page.Key...),
		Offset:     page.Offset,
		Limit:      page.Limit,
		CountTotal: page.CountTotal,
		Reverse:    page.Reverse,
	}
}

func fromSDKPage(page *querytypes.PageResponse) PageResponse {
	if page == nil {
		return PageResponse{}
	}
	return PageResponse{NextKey: append([]byte(nil), page.NextKey...), Total: page.Total}
}

func fileFromProto(file *storagetypes.File) File {
	if file == nil {
		return File{}
	}
	return File{
		ID:                file.Fid,
		MerkleRoot:        file.Merkle,
		Creator:           file.Creator,
		SubscriptionID:    file.Subscription,
		Status:            file.Status,
		Size:              file.FileSize,
		Replicas:          file.Replicas,
		Providers:         append([]string(nil), file.Providers...),
		UnstableProviders: append([]string(nil), file.UnstableProviders...),
		Start:             file.Start,
	}
}

func treeNodeFromProto(node *filetreetypes.TreeNode) *FileTreeNode {
	return &FileTreeNode{NodeType: node.NodeType, Contents: node.Contents, Viewers: node.Viewers, Editors: node.Editors}
}
