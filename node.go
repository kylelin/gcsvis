package main

import (
    "cloud.google.com/go/storage"
    v1 "google.golang.org/api/storage/v1"
)

type Node struct {
    // name of bucket that current node belongs to
    Bucket string `json:"bucket"`

    // any of OBJ, DIR
    Ntype string `json:"ntype"`

    // object name or,
    // prefix name in case of DIR
    Name string `json:"name"`

    // Fully qualified path name
    // Example, gs://test/test1.txt
    // FQPN is `test/test1.txt` where Name is `test1.txt`
    FQPN string `json:"fqpn"`

    ACL []storage.ACLRule `json:"acl"`

    Lifecycle *v1.BucketLifecycle `json:"lifecycle"`

    Size int64 `json:"size"`
    Owner string `json:"owner"`
}
