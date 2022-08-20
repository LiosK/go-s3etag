package main

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {
	chunksize := flagChunksize(8 * 1024 * 1024)
	flag.Var(&chunksize, "chunksize", "multipart_chunksize used for upload in bytes or with a size suffix KB, MB, GB, or TB")
	flag.Parse()

	nErrors := 0
	for _, filename := range flag.Args() {
		etag, err := computeEtag(filename, int64(chunksize))
		if err == nil {
			fmt.Printf("%-39s %s\n", etag, filename)
		} else {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			nErrors += 1
		}
	}

	if nErrors > 0 {
		os.Exit(1)
	}
}

type flagChunksize int64

// Parse chunksize argument.
func (v *flagChunksize) Set(s string) error {
	scale := 0
	if strings.HasSuffix(s, "TB") {
		scale = 40
	} else if strings.HasSuffix(s, "GB") {
		scale = 30
	} else if strings.HasSuffix(s, "MB") {
		scale = 20
	} else if strings.HasSuffix(s, "KB") {
		scale = 10
	}
	if scale > 0 {
		s = s[:len(s)-2]
	}

	i, err := strconv.ParseUint(s, 10, 63-scale)
	if err != nil {
		return err
	}

	*v = flagChunksize(i << scale)
	if *v < 1 {
		return errors.New("non-positive chunksize")
	}

	return nil
}

// Prints the chuksize value, with a size prefix when appropriate.
func (v *flagChunksize) String() string {
	if v == nil {
		return "nil"
	} else if *v%(1<<40) == 0 {
		return fmt.Sprintf("%dTB", *v>>40)
	} else if *v%(1<<30) == 0 {
		return fmt.Sprintf("%dGB", *v>>30)
	} else if *v%(1<<20) == 0 {
		return fmt.Sprintf("%dMB", *v>>20)
	} else if *v%(1<<10) == 0 {
		return fmt.Sprintf("%dKB", *v>>10)
	} else {
		return fmt.Sprintf("%d", *v)
	}
}

// Compute Etag for a file.
func computeEtag(filename string, chunksize int64) (string, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	dgstPart := md5.New()
	_, err = io.CopyN(dgstPart, fh, chunksize)
	if err != nil && err != io.EOF {
		return "", err
	}

	sum := dgstPart.Sum(nil)
	dgstPart.Reset()
	written, err := io.CopyN(dgstPart, fh, chunksize)
	if err != nil && err != io.EOF {
		return "", err
	} else if written == 0 {
		return fmt.Sprintf("%x", sum), nil
	}

	count := 1
	dgstWhole := md5.New()
	dgstWhole.Write(sum)

	for written > 0 {
		count += 1
		dgstWhole.Write(dgstPart.Sum(nil))
		dgstPart.Reset()
		written, err = io.CopyN(dgstPart, fh, chunksize)
		if err != nil && err != io.EOF {
			return "", err
		}
	}

	return fmt.Sprintf("%x-%d", dgstWhole.Sum(nil), count), nil
}
