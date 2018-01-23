package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/user"
	"path"
	"runtime"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	"github.com/jacobsa/syncutil"
)

const (
	createRateHz        = 10
	verifyAtAge         = 10 * time.Millisecond
	verifyAgainAtAge    = 5 * time.Minute
	andAgainAtAge       = 60 * time.Minute
	perStageParallelism = 128

	objectNamePrefix = "blobs/"
)

var fBucket = flag.String("bucket", "", "")

type record struct {
	sha1         [sha1.Size]byte
	creationTime time.Time
}

func getBucket() (b gcs.Bucket, err error) {
	if *fBucket == "" {
		err = errors.New("You must set --bucket.")
		return
	}

	// Create the HTTP client.
	const scope = gcs.Scope_FullControl
	tokenSrc, err := google.DefaultTokenSource(context.Background(), scope)
	if err != nil {
		err = fmt.Errorf("DefaultTokenSource: %v", err)
		return
	}

	// Set up a GCS connection.
	cfg := &gcs.ConnConfig{
		TokenSource:     tokenSrc,
		MaxBackoffSleep: 60 * time.Minute,
	}

	conn, err := gcs.NewConn(cfg)
	if err != nil {
		err = fmt.Errorf("gcs.NewConn: %v", err)
		return
	}

	// Open the bucket.
	b, err = conn.OpenBucket(context.Background(), &gcs.OpenBucketOptions{Name: *fBucket})
	if err != nil {
		err = fmt.Errorf("OpenBucket: %v", err)
		return
	}

	return
}

func randBytes(
	r *rand.Rand,
	n int) (b []byte) {
	b = make([]byte, n)
	fullWords := n / 4
	remainder := n % 4

	for i := 0; i < fullWords; i++ {
		u32 := r.Uint32()
		b[4*i+0] = byte((u32 >> 0) & 0xff)
		b[4*i+1] = byte((u32 >> 8) & 0xff)
		b[4*i+2] = byte((u32 >> 16) & 0xff)
		b[4*i+3] = byte((u32 >> 24) & 0xff)
	}

	for i := 0; i < remainder; i++ {
		b[4*fullWords+i] = byte(r.Uint32())
	}

	return
}

func makeSeed() (seed int64) {
	var buf [8]byte
	_, err := io.ReadFull(cryptorand.Reader, buf[:])
	if err != nil {
		panic(err)
	}

	seed = (int64(buf[0])>>1)<<56 |
		int64(buf[1])<<48 |
		int64(buf[2])<<40 |
		int64(buf[3])<<32 |
		int64(buf[4])<<24 |
		int64(buf[5])<<16 |
		int64(buf[6])<<8 |
		int64(buf[7])<<0

	return
}

func createObjects(
	ctx context.Context,
	bucket gcs.Bucket,
	out chan<- record) (err error) {
	throttle := time.Tick(time.Second / createRateHz)

	// Set up a worker function.
	worker := func(ctx context.Context) (err error) {
		randSrc := rand.New(rand.NewSource(makeSeed()))
		for {
			// Choose a random size (every once in awhile making sure we see the
			// max), and generate content of that size.
			const maxSize = 1 << 24
			var size int
			if randSrc.Int31n(100) == 0 {
				size = maxSize
			} else {
				size = int(randSrc.Int31n(maxSize + 1))
			}

			content := randBytes(randSrc, size)

			// Compute hashes and checksums.
			sha1 := sha1.Sum(content)
			crc32c := *gcsutil.CRC32C(content)

			// Wait for permission to proceed.
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case <-throttle:
			}

			// Create an object.
			req := &gcs.CreateObjectRequest{
				Name:     fmt.Sprintf("%s%x", objectNamePrefix, sha1),
				Contents: bytes.NewReader(content),
				CRC32C:   &crc32c,

				Metadata: map[string]string{
					"expected_sha1":   fmt.Sprintf("%x", sha1),
					"expected_crc32c": fmt.Sprintf("%#08x", crc32c),
				},
			}

			var o *gcs.Object
			o, err = bucket.CreateObject(ctx, req)
			if err != nil {
				err = fmt.Errorf("CreateObject(%q): %v", req.Name, err)
				return
			}

			log.Printf("Created object %q.", req.Name)

			// Check the object.
			if o.Name != req.Name {
				err = fmt.Errorf(
					"Name mismatch: %q vs. %q",
					o.Name,
					req.Name)

				return
			}

			if o.CRC32C != crc32c {
				err = fmt.Errorf(
					"Object %q CRC mismatch: %#08x vs. %#08x",
					o.Name,
					o.CRC32C,
					crc32c)

				return
			}

			// Write out the record.
			r := record{
				sha1:         sha1,
				creationTime: time.Now(),
			}

			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case out <- r:
			}
		}
	}

	// Run a bunch of workers.
	b := syncutil.NewBundle(ctx)
	for i := 0; i < perStageParallelism; i++ {
		b.Add(worker)
	}

	err = b.Join()
	return
}

func verifyObjects(
	ctx context.Context,
	bucket gcs.Bucket,
	verifyAfter time.Duration,
	in <-chan record,
	out chan<- record) (err error) {
	// Set up a worker function.
	worker := func(ctx context.Context) (err error) {
		for r := range in {
			name := fmt.Sprintf("%s%x", objectNamePrefix, r.sha1)

			// Wait until it is time.
			wakeTime := r.creationTime.Add(verifyAfter)
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case <-time.After(wakeTime.Sub(time.Now())):
			}

			// Attempt to read the object.
			var contents []byte
			contents, err = gcsutil.ReadObject(ctx, bucket, name)
			if err != nil {
				err = fmt.Errorf("ReadObject(%q): %v", name, err)
				return
			}

			// Check the contents.
			actual := sha1.Sum(contents)
			if actual != r.sha1 {
				err = fmt.Errorf(
					"SHA1 mismatch for %q: %x vs. %x",
					name,
					actual,
					r.sha1)

				return
			}

			log.Printf("Verified object %q.", name)

			// Pass on the record if we've been asked to.
			if out != nil {
				select {
				case <-ctx.Done():
					err = ctx.Err()
					return

				case out <- r:
				}
			}
		}

		return
	}

	// Run a bunch of workers.
	b := syncutil.NewBundle(ctx)
	for i := 0; i < perStageParallelism; i++ {
		b.Add(worker)
	}

	err = b.Join()
	return
}

func run() (err error) {
	runtime.GOMAXPROCS(4)

	// Grab the bucket.
	bucket, err := getBucket()
	if err != nil {
		err = fmt.Errorf("getBucket: %v", err)
		return
	}

	b := syncutil.NewBundle(context.Background())

	// Create objects.
	toVerify := make(chan record, 2*createRateHz*(verifyAtAge/time.Second))
	b.Add(func(ctx context.Context) (err error) {
		defer close(toVerify)
		err = createObjects(ctx, bucket, toVerify)
		if err != nil {
			err = fmt.Errorf("createObjects: %v", err)
			return
		}

		return
	})

	// Verify them after awhile.
	toVerifyAgain := make(
		chan record,
		2*createRateHz*(verifyAgainAtAge/time.Second))

	b.Add(func(ctx context.Context) (err error) {
		defer close(toVerifyAgain)
		err = verifyObjects(
			ctx,
			bucket,
			verifyAtAge,
			toVerify,
			toVerifyAgain)

		if err != nil {
			err = fmt.Errorf("verifyObjects: %v", err)
			return
		}

		return
	})

	// And again.
	andOnceMore := make(
		chan record,
		2*createRateHz*(andAgainAtAge/time.Second))

	b.Add(func(ctx context.Context) (err error) {
		defer close(andOnceMore)
		err = verifyObjects(
			ctx,
			bucket,
			verifyAgainAtAge,
			toVerifyAgain,
			andOnceMore)

		if err != nil {
			err = fmt.Errorf("verifyObjects: %v", err)
			return
		}

		return
	})

	// And again.
	b.Add(func(ctx context.Context) (err error) {
		err = verifyObjects(
			ctx,
			bucket,
			verifyAgainAtAge,
			andOnceMore,
			nil)

		if err != nil {
			err = fmt.Errorf("verifyObjects: %v", err)
			return
		}

		return
	})

	err = b.Join()
	return
}

func getHomedir() (homedir string) {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}

	homedir = u.HomeDir
	return
}

func main() {
	flag.Parse()

	// Include more detailed output in stderr logs.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	// Set up the GCS log.
	gcsLog, err := os.OpenFile(
		path.Join(getHomedir(), ".fuzzer.gcs.log"),
		os.O_RDWR|os.O_APPEND|os.O_CREATE,
		0600)

	if err != nil {
		log.Fatalf("OpenFile: %v", err)
	}

	defer gcsLog.Close()

	// Run.
	err = run()
	if err != nil {
		log.Fatal(err)
	}
}
