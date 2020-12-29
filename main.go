package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Config struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	Insecure  bool   `json:"insecure"`
}

const config = `./config.json`

func main() {
	var sc S3Config

	f, err := os.Open(config)
	if err != nil {
		log.Fatal("open config file error: ", err)
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal("read config file error: ", err)
	}
	if err := json.Unmarshal(b, &sc); err != nil {
		log.Fatal("parsing config file error: ", err)
	}

	minioClient, err := minio.New(sc.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(sc.AccessKey, sc.SecretKey, ""),
		Secure: !sc.Insecure,
	})
	if err != nil {
		log.Fatal("new minio client err: ", err)
	}

	markMap := make(map[string]struct{})
	deleteCh := make(chan minio.ObjectInfo)

	ctx := context.TODO()
	for object := range minioClient.ListObjects(ctx, sc.Bucket, minio.ListObjectsOptions{
		UseV1:     true,
		Recursive: true,
	}) {
		if path.Base(object.Key) == "deletion-mark.json" {
			markMap[strings.Split(object.Key, "/")[0]] = struct{}{}
		}
	}

	fmt.Println("Ready to delete folder num:", len(markMap))
	bf, err := os.Create(fmt.Sprintf("deletion-mark-%d.backup", time.Now().Unix()))
	if err != nil {
		log.Fatal("create backup file err: ", err)
	}
	defer bf.Close()

	w := bufio.NewWriter(bf)
	for k := range markMap {
		fmt.Fprintln(w, k)
	}
	if err := w.Flush(); err != nil {
		log.Fatal("flush backup file err: ", err)
	}

	go func() {
		for object := range minioClient.ListObjects(ctx, sc.Bucket, minio.ListObjectsOptions{
			UseV1:     true,
			Recursive: true,
		}) {
			if _, ok := markMap[strings.Split(object.Key, "/")[0]]; ok {
				deleteCh <- object
			}
		}
		close(deleteCh)
	}()

	errCh := minioClient.RemoveObjects(ctx, sc.Bucket, deleteCh, minio.RemoveObjectsOptions{})
	for range errCh {
		log.Fatal("remove objects err: ", <-errCh)
	}
}
