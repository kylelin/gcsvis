package main

import (
    "fmt"
    "net/http"
    "runtime"

    "golang.org/x/net/context"

    "google.golang.org/api/option"
    "google.golang.org/api/transport"
    v1 "google.golang.org/api/storage/v1"
)

const (
    UserAgent = "gcloud-golang-storage/20170618"

    // ScopeFullControl grants permissions to manage your
    // data and permissions in Google Cloud Storage.
    ScopeFullControl = v1.DevstorageFullControlScope

    // ScopeReadOnly grants permissions to
    // view your data in Google Cloud Storage.
    ScopeReadOnly = v1.DevstorageReadOnlyScope

    // ScopeReadWrite grants permissions to manage your
    // data in Google Cloud Storage.
    ScopeReadWrite = v1.DevstorageReadWriteScope
)

// Go Version 
var GoVersion = runtime.Version()

// Source Version 
var SourceVersion = "v0.0.1"

var xGoogHeader = fmt.Sprintf("gl-go/%s gccl/%s", GoVersion, SourceVersion)

func setClientHeader(headers http.Header) {
    headers.Set("x-goog-api-client", xGoogHeader)
}

// ThinClient is a client for interacting with Google Cloud Storage.
type ThinClient struct {
    hc  *http.Client
    v1 *v1.Service
}

// NewClient creates a new Google Cloud Storage thin client.
func NewThinClient(ctx context.Context, opts ...option.ClientOption) (*ThinClient, error) {
    o := []option.ClientOption{
        option.WithScopes(ScopeFullControl),
        option.WithUserAgent(UserAgent),
    }
    opts = append(o, opts...)
    hc, ep, err := transport.NewHTTPClient(ctx, opts...)
    if err != nil {
        return nil, fmt.Errorf("dialing: %v", err)
    }
    v1Service, err := v1.New(hc)
    if err != nil {
        return nil, fmt.Errorf("storage client: %v", err)
    }
    if ep != "" {
        v1Service.BasePath = ep
    }
    return &ThinClient{
        hc:  hc,
        v1:  v1Service,
    }, nil
}

// LifeCycle returns the lifecycle rules for given bucket.
// Directly invoking storage v1 API.
func (tc *ThinClient) Lifecycle(ctx context.Context, bucketName string) (*v1.BucketLifecycle, error) {
    req := tc.v1.Buckets.Get(bucketName).Projection("full")
    setClientHeader(req.Header())
    resp, err := req.Context(ctx).Do()
    if err != nil {
        return nil, err
    }
    return resp.Lifecycle, nil
}
