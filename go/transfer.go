package atlas

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/atlas-depin/atlas-sdk/go/internal/merkle"
	filetreetypes "github.com/atlas-depin/atlas/x/filetree/types"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

const defaultProviderTimeout = 10 * time.Minute

// Upload creates the 1 KiB-chunked Atlas commitment, stages the file on
// chain, waits for that transaction to commit, then uploads the file to
// registered providers until the requested minimum succeeds.
func (c *Client) Upload(ctx context.Context, wallet *Wallet, request UploadRequest) (*UploadResult, error) {
	return c.Storage.Upload(ctx, wallet, request)
}

// Upload creates the on-chain storage record and optional filetree node in one
// transaction, waits for it to commit, then uploads the bytes to providers.
// Sending bytes only after commit satisfies the provider's staged-file check.
func (h *StorageHandler) Upload(ctx context.Context, wallet *Wallet, request UploadRequest) (*UploadResult, error) {
	if h == nil || h.client == nil {
		return nil, fmt.Errorf("storage handler is not initialized")
	}
	c := h.client
	if wallet == nil {
		return nil, fmt.Errorf("wallet is required")
	}
	if request.FID == "" {
		return nil, fmt.Errorf("file ID is required")
	}
	if request.Source == nil {
		return nil, fmt.Errorf("upload source is required")
	}
	if request.Replicas == 0 {
		request.Replicas = 3
	}
	if request.Replicas < 2 {
		return nil, fmt.Errorf("Atlas requires at least two file replicas")
	}
	if request.MinSuccessfulUploads == 0 {
		request.MinSuccessfulUploads = 1
	}
	if request.MinSuccessfulUploads < 1 {
		return nil, fmt.Errorf("minimum successful uploads must be positive")
	}
	if request.MinSuccessfulUploads > int(request.Replicas) {
		return nil, fmt.Errorf("minimum successful uploads cannot exceed requested replicas")
	}
	if request.ProviderTimeout == 0 {
		request.ProviderTimeout = defaultProviderTimeout
	}

	if _, err := request.Source.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind upload source: %w", err)
	}
	commitment, err := merkle.Root(request.Source)
	if err != nil {
		return nil, fmt.Errorf("calculate file commitment: %w", err)
	}
	if request.Size > 0 && request.Size != commitment.Size {
		return nil, fmt.Errorf("upload size is %d bytes but source contains %d bytes", request.Size, commitment.Size)
	}
	if _, err := request.Source.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind upload source after commitment: %w", err)
	}

	messages := []sdk.Msg{&storagetypes.MsgPostFile{
		Creator:      wallet.Address(),
		Fid:          request.FID,
		Merkle:       commitment.Root,
		FileSize:     commitment.Size,
		Replicas:     request.Replicas,
		Subscription: request.SubscriptionID,
	}}
	if request.FileTree != nil {
		if request.FileTree.Path == "" {
			return nil, fmt.Errorf("filetree path is required when posting a filetree node")
		}
		messages = append(messages, &filetreetypes.MsgPostNode{
			Creator:  wallet.Address(),
			Path:     request.FileTree.Path,
			NodeType: request.FileTree.NodeType,
			Contents: request.FileTree.Contents,
			Viewers:  request.FileTree.Viewers,
			Editors:  request.FileTree.Editors,
		})
	}

	txResult, err := c.signAndBroadcast(ctx, wallet, messages...)
	if err != nil {
		return nil, err
	}
	if _, err := c.WaitForTx(ctx, txResult.Hash); err != nil {
		return nil, fmt.Errorf("wait for staged file transaction: %w", err)
	}

	providers, err := c.Providers(ctx)
	if err != nil {
		return nil, err
	}
	return c.uploadToProviders(ctx, request, commitment, txResult, providers)
}

func (c *Client) uploadToProviders(ctx context.Context, request UploadRequest, commitment *merkle.Result, txResult *TxResult, providers []Provider) (*UploadResult, error) {
	result := &UploadResult{Transaction: txResult, MerkleRoot: append([]byte(nil), commitment.Root...), Size: commitment.Size}
	errs := make([]error, 0)
	for _, provider := range providers {
		if provider.Hostname == "" {
			continue
		}
		if len(result.Providers) >= request.MinSuccessfulUploads {
			break
		}
		if _, err := request.Source.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind upload source: %w", err)
		}
		providerCtx, cancel := context.WithTimeout(ctx, request.ProviderTimeout)
		err := c.uploadToProvider(providerCtx, provider.Hostname, request.FID, request.Source)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", provider.Hostname, err))
			continue
		}
		result.Providers = append(result.Providers, provider.Hostname)
	}
	if len(result.Providers) < request.MinSuccessfulUploads {
		return nil, fmt.Errorf("uploaded to %d/%d required providers: %w", len(result.Providers), request.MinSuccessfulUploads, errors.Join(errs...))
	}
	return result, nil
}

func (c *Client) uploadToProvider(ctx context.Context, hostname, fid string, source io.Reader) error {
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer pipeWriter.Close()
		if err := multipartWriter.WriteField("fid", fid); err != nil {
			_ = pipeWriter.CloseWithError(err)
			done <- err
			return
		}
		fileWriter, err := multipartWriter.CreateFormFile("file", fid)
		if err == nil {
			_, err = io.Copy(fileWriter, source)
		}
		if closeErr := multipartWriter.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
		}
		done <- err
	}()

	endpoint, err := providerEndpoint(hostname, "upload", fid)
	if err != nil {
		pipeReader.Close()
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, pipeReader)
	if err != nil {
		pipeReader.Close()
		return fmt.Errorf("build provider upload request: %w", err)
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	response, err := c.httpClient().Do(req)
	if err != nil {
		pipeReader.Close()
		return fmt.Errorf("upload to provider: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("provider returned %s", response.Status)
	}
	if writeErr := <-done; writeErr != nil {
		return fmt.Errorf("stream provider upload: %w", writeErr)
	}
	return nil
}

// Download streams fid from an on-chain listed provider into destination and
// verifies the received bytes against the file's on-chain Merkle root.
func (c *Client) Download(ctx context.Context, fid string, destination io.Writer) (*DownloadResult, error) {
	if destination == nil {
		return nil, fmt.Errorf("download destination is required")
	}
	file, err := c.File(ctx, fid)
	if err != nil {
		return nil, err
	}
	if len(file.Providers) == 0 {
		return nil, fmt.Errorf("file %q has no active providers", fid)
	}
	providers, err := c.Providers(ctx)
	if err != nil {
		return nil, err
	}
	hostnames := make(map[string]string, len(providers))
	for _, provider := range providers {
		hostnames[provider.Address] = provider.Hostname
	}

	errs := make([]error, 0)
	for _, address := range file.Providers {
		hostname := hostnames[address]
		if hostname == "" {
			errs = append(errs, fmt.Errorf("provider %s has no hostname", address))
			continue
		}
		result, err := c.downloadFromProvider(ctx, hostname, fid, file.MerkleRoot, destination)
		if err == nil {
			return result, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", hostname, err))
	}
	return nil, fmt.Errorf("download %q from listed providers: %w", fid, errors.Join(errs...))
}

func (c *Client) downloadFromProvider(ctx context.Context, hostname, fid, expectedRoot string, destination io.Writer) (*DownloadResult, error) {
	// Verify into a temporary file first. This makes trying the next provider
	// safe: no unverified or partial bytes are written to destination.
	temporary, err := os.CreateTemp("", "atlas-download-*")
	if err != nil {
		return nil, fmt.Errorf("create download staging file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	defer temporary.Close()

	result, err := c.downloadAndVerify(ctx, hostname, fid, expectedRoot, temporary)
	if err != nil {
		return nil, err
	}
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind verified download: %w", err)
	}
	if _, err := io.Copy(destination, temporary); err != nil {
		return nil, fmt.Errorf("write verified download: %w", err)
	}
	return result, nil
}

func (c *Client) downloadAndVerify(ctx context.Context, hostname, fid, expectedRoot string, destination io.Writer) (*DownloadResult, error) {
	endpoint, err := providerEndpoint(hostname, "download", fid)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build provider download request: %w", err)
	}
	response, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("download from provider: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("provider returned %s", response.Status)
	}
	commitment, err := merkle.Root(io.TeeReader(response.Body, destination))
	if err != nil {
		return nil, fmt.Errorf("receive and verify provider download: %w", err)
	}
	expected, err := hex.DecodeString(expectedRoot)
	if err != nil {
		return nil, fmt.Errorf("decode on-chain Merkle root: %w", err)
	}
	if !equalBytes(expected, commitment.Root) {
		return nil, fmt.Errorf("provider content does not match the on-chain Merkle root")
	}
	return &DownloadResult{Provider: hostname, Size: commitment.Size, Verified: true}, nil
}

func (c *Client) httpClient() *http.Client {
	if c.config.HTTPClient != nil {
		return c.config.HTTPClient
	}
	return http.DefaultClient
}

func providerEndpoint(hostname, operation, fid string) (string, error) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", fmt.Errorf("provider hostname is required")
	}
	if !strings.Contains(hostname, "://") {
		hostname = "https://" + hostname
	}
	endpoint, err := url.Parse(hostname)
	if err != nil || endpoint.Host == "" {
		return "", fmt.Errorf("invalid provider hostname %q", hostname)
	}
	switch operation {
	case "upload":
		endpoint.Path = path.Join(endpoint.Path, "api", "v1", "upload")
	case "download":
		if fid == "" || fid != path.Base(fid) || strings.Contains(fid, "..") {
			return "", fmt.Errorf("invalid file ID %q", fid)
		}
		endpoint.Path = path.Join(endpoint.Path, "api", "v1", "download", fid)
	default:
		return "", fmt.Errorf("unsupported provider operation %q", operation)
	}
	return endpoint.String(), nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var difference byte
	for i := range a {
		difference |= a[i] ^ b[i]
	}
	return difference == 0
}
