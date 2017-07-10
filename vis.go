package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strings"

    "cloud.google.com/go/storage"
    "github.com/gorilla/mux"
    "golang.org/x/net/context"
    "google.golang.org/api/iterator"
)

var CLIENT *storage.Client
var THINCLIENT *ThinClient

func InitClient() {
    ctx := context.Background()
    var err error
    CLIENT, err = storage.NewClient(ctx)
    if err != nil {
        log.Panic(err)
    }
    THINCLIENT, err = NewThinClient(ctx)
    if err != nil {
        log.Panic(err)
    }
}

func GetNodeEndpoint(w http.ResponseWriter, req *http.Request) {
    params := mux.Vars(req)
    prefix := req.FormValue("prefix")
    nodes, err := Explore(CLIENT, THINCLIENT, params["bucket"], prefix)
    if err != nil {
        log.Fatal(err)
    }
    json.NewEncoder(w).Encode(nodes)
}

func main() {
    secret := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if secret == "" {
        fmt.Fprintf(os.Stderr, "GOOGLE_APPLICATION_CREDENTIALS environment variable must be set.\n")
        os.Exit(1)
    }

    InitClient()

    router := mux.NewRouter()
    router.HandleFunc("/node/{bucket}", GetNodeEndpoint).Methods("GET")
    router.PathPrefix("/").Handler(http.FileServer(http.Dir("./static/")))
    http.Handle("/", router)

    log.Fatal(http.ListenAndServe(":33333", nil))
}

// Fetch children of given path
// if prefix is nil, this function will return direct children of given bucket.
// otherwise, return direct children of given bucket/prefix
// 
// A prefix should be ended with delimiter '/'.
func Explore(client *storage.Client, thinClient *ThinClient, bucket string, prefix string) ([]*Node, error) {
    // Combine bucket lifecycle to each node
    ctx := context.Background()
    lifecycle, err := thinClient.Lifecycle(ctx, bucket)
    if err != nil {
        return nil, err
    }

    objs, err := listq(client, bucket, prefix, "/", false, true)
    if err != nil {
        return nil, err
    }
    var nodes []*Node
    for _, element := range objs {
        var node Node
        node.Bucket = bucket
        node.Lifecycle = lifecycle
        if element.Name == "" && element.Prefix != "" {
            node.Ntype = "DIR"
            node.Name = element.Prefix
            node.FQPN = element.Prefix
        } else {
            node.Ntype = "OBJ"
            pathSlice := strings.Split(element.Name, "/")
            pathLength := len(pathSlice)
            node.Name = pathSlice[pathLength - 1]
            node.FQPN = element.Name
        }
        node.ACL = element.ACL
        node.Owner = element.Owner
        node.Size = element.Size

        nodes = append(nodes, &node)
    }
    return nodes, nil
}

// List buckets of given project.
// See also https://godoc.org/cloud.google.com/go/storage#BucketAttrs.
func listp(client *storage.Client, projectId string) ([]*storage.BucketAttrs, error) {
    ctx := context.Background()
    var buckets []*storage.BucketAttrs
    it := client.Buckets(ctx, projectId)
    for {
        battrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, err
        }
        buckets = append(buckets, battrs)
    }
    return buckets, nil
}

// List objects of given bucket.
// See also https://godoc.org/cloud.google.com/go/storage#ObjectAttrs
func listb(client *storage.Client, bucketName string) ([]*storage.ObjectAttrs, error) {
    ctx := context.Background()
    var objects []*storage.ObjectAttrs
    it := client.Bucket(bucketName).Objects(ctx, nil)
    for {
        oattrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, err
        }
        objects = append(objects, oattrs)
    }
    return objects, nil
}

// List objects give bucket and query conditions(prefix, delimiter, versions)
// See also https://godoc.org/cloud.google.com/go/storage#Query.
// Examples: 
//  - fetch direct children of bucket `test`
//      listq(client, "test", "", "/", false, true)
//  - fetch direct children of bucket `test` and prefix `test-1`
//      listq(client, "test", "test-1/", "/", false, true)
//  - fetch direct and indirect children of bucket `test`, which is same as listb
//      listq(client, "test", "", "", false, true)
// selfIgnore is used for filtering out empty prefix self from query results.
func listq(client *storage.Client, bucketName string, prefix string, delimiter string, versions bool, selfIgnore bool) ([]*storage.ObjectAttrs, error){
    ctx := context.Background()
    var objects []*storage.ObjectAttrs
    it := client.Bucket(bucketName).Objects(ctx, &storage.Query{
        Prefix: prefix,
        Delimiter: delimiter,
        Versions: versions,
    })
    for {
        oattrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, err
        }
        if selfIgnore == true && oattrs.Prefix == prefix && oattrs.Prefix != "" {
            continue
        }
        objects = append(objects, oattrs)
    }
    return objects, nil
}
