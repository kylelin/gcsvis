package main

import (
    "fmt"
    "os"
    "bytes"
    "testing"

    "golang.org/x/net/context"
    "cloud.google.com/go/storage"

    "github.com/satori/go.uuid"
)

func RequireEnvVar() (bool) {
    secret := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project := os.Getenv("GOOGLE_CLOUD_PROJECT")
    if secret == "" {
        fmt.Fprintf(os.Stderr, "GOOGLE_APPLICATION_CREDENTIALS environment variable must be set.\n")
        return false
    }
    if project == "" {
        fmt.Fprintf(os.Stderr, "GOOGLE_CLOUD_PROJECT environment variable must be set.\n")
        return false
    }
    return true
}

func InitClient() (*storage.Client, error) {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, err
    }
    return client, nil
}

func WriteDummyObject(client *storage.Client, bucketName string, objectName string) (error) {
    ctx := context.Background()
    obj := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
    obj.ContentType = "text/plain"
    if _, err := obj.Write([]byte("dummy object")); err != nil {
        return err
    }
    if err := obj.Close(); err != nil {
        return err
    }
    return nil
}

// List buckets of given project
// Since the buckets may variance due to project, it is convenient to create one dummy bucket and 
// test expected bucket containment from given project.
func TestListProject(t *testing.T) {
    if RequireEnvVar() {
        client, err := InitClient()
        if err != nil {
            t.Error(err)
        }

        // Setup: create dummy bucket
        ctx := context.Background()
        projectId := os.Getenv("GOOGLE_CLOUD_PROJECT")
        expectedBucketName := uuid.NewV4().String()
        if err := client.Bucket(expectedBucketName).Create(ctx, projectId, nil); err != nil {
            t.Fatalf("Setup failed!")
        }

        buckets, err := listp(client, projectId)
        if err != nil {
            t.Error(err)
        }

        isContainment := false
        for _, element := range buckets{
            if element.Name == expectedBucketName {
                isContainment = true
            }
        }

        if !isContainment {
            t.Fatalf("Failed to list project.")
        }

        // Teardown: delete dummy bucket
        if err := client.Bucket(expectedBucketName).Delete(ctx); err != nil {
            t.Error(err)
        }
    } else {
        t.Fatalf("Env variables undefined!")
    }
}

// List objects of given project and bucket
func TestListBucket(t *testing.T) {
    if RequireEnvVar() {
        client, err := InitClient()
        if err != nil {
            t.Error(err)
        }

        // Setup: create dummy bucket
        ctx := context.Background()
        projectId := os.Getenv("GOOGLE_CLOUD_PROJECT")
        expectedBucketName := uuid.NewV4().String()
        if err := client.Bucket(expectedBucketName).Create(ctx, projectId, nil); err != nil {
            t.Fatalf("Setup failed!")
        }
        expectedObject := uuid.NewV4().String()
        if err := WriteDummyObject(client, expectedBucketName, expectedObject); err != nil {
            t.Fatalf("Setup failed!")
        }

        objects, err := listb(client, expectedBucketName)
        if err != nil {
            t.Error(err)
        }

        isContainment := false
        for _, element := range objects{
            if element.Name == expectedObject {
                isContainment = true
            }
        }

        if !isContainment {
            t.Fatalf("Failed to list project.")
        }

        // Teardown: delete dummy bucket
        if err := client.Bucket(expectedBucketName).Object(expectedObject).Delete(ctx); err != nil {
            t.Error(err)
        }
        if err := client.Bucket(expectedBucketName).Delete(ctx); err != nil {
            t.Error(err)
        }
    } else {
        t.Fatalf("Env variables undefined!")
    }
}

// List objects of given project and bucket with query
func TestListQuery(t *testing.T) {
    if RequireEnvVar() {
        client, err := InitClient()
        if err != nil {
            t.Error(err)
        }

        // Setup: create dummy bucket
        ctx := context.Background()
        projectId := os.Getenv("GOOGLE_CLOUD_PROJECT")
        expectedBucketName := uuid.NewV4().String()
        if err := client.Bucket(expectedBucketName).Create(ctx, projectId, nil); err != nil {
            t.Fatalf("Setup failed!")
        }
        expectedObjectPrefix := uuid.NewV4().String()
        expectedObjectName := uuid.NewV4().String()
        expectedObjectPrefix2 := uuid.NewV4().String()
        var expectedObject bytes.Buffer
        expectedObject.WriteString(expectedObjectPrefix)
        expectedObject.WriteString("/")
        expectedObjectPrefixStr := expectedObject.String()
        expectedObject.WriteString(expectedObjectName)
        var expectedObject2 bytes.Buffer
        expectedObject2.WriteString(expectedObjectPrefix)
        expectedObject2.WriteString("/")
        expectedObject2.WriteString(expectedObjectPrefix2)
        expectedObject2.WriteString("/")
        expectedObjectPrefixStr2 := expectedObject2.String()
        expectedObject2.WriteString(expectedObjectName)
        if err := WriteDummyObject(client, expectedBucketName, expectedObject.String()); err != nil {
            t.Fatalf("Setup failed!")
        }
        if err := WriteDummyObject(client, expectedBucketName, expectedObject2.String()); err != nil {
            t.Fatalf("Setup failed!")
        }

        // Setup result:
        // gs://expectedBucketName/
        //          |________ expectedObjectPrefix/
        //                          |__________ expectedObjectName
        //                          |__________ expectedObjectPrefix2/
        //                                                  |___________ expectedObjectName

        // Test: fetch direct children of bucket
        // Expected: expectedObjectPrefix
        objects_1, err := listq(client, expectedBucketName, "", "/", false, true)
        for _, element := range objects_1 {
            if element.Prefix != expectedObjectPrefixStr {
                t.Errorf("Unexpected direct children listup.")
            }
        }

        // Test: fetch direct children of any given prefix 
        // Expected: expectedObjectName and expectedObjectPrefix2/
        isObjectMatch := false
        isPrefixMatch := false
        objects_2, err := listq(client, expectedBucketName, expectedObjectPrefixStr, "/", false, true)
        for _, element := range objects_2 {
            if element.Name == expectedObject.String() {
                isObjectMatch = true
            }
            if element.Prefix == expectedObjectPrefixStr2 {
                isPrefixMatch = true
            }
        }

        if !isObjectMatch || !isPrefixMatch {
            t.Errorf("Unpexpected direct children listup with given prefix.")
        }

        // Test: fetch all direct and indirect children including prefix and object 
        // Expected: expectedObjectName, expectedObjectName from different prefix depth 
        isFirstMatch := false
        isSecondMatch := false
        objects_3, err := listq(client, expectedBucketName, "", "", false, true)
        for _, element := range objects_3 {
            if element.Name == expectedObject.String() {
                isFirstMatch = true
            }
            if element.Name == expectedObject2.String() {
                isSecondMatch = true
            }
        }

        if !isFirstMatch || !isSecondMatch {
            t.Errorf("Unexpected object query.")
        }

        // Teardown: delete dummy bucket
        if err := client.Bucket(expectedBucketName).Object(expectedObject2.String()).Delete(ctx); err != nil {
            t.Error(err)
        }
        if err := client.Bucket(expectedBucketName).Object(expectedObject.String()).Delete(ctx); err != nil {
            t.Error(err)
        }
        if err := client.Bucket(expectedBucketName).Delete(ctx); err != nil {
            t.Error(err)
        }
    } else {
        t.Fatalf("Env variables undefined!")
    }
}
