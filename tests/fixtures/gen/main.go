// ltxgen encodes a raw SQLite database file into a single snapshot LTX file
// using the local superfly/ltx library. This is literstream's fixture
// generator: `ltx encode-db` in the checkout is stale (hardcodes Version:1,
// which the v3 library rejects), so we drive the encoder directly here.
//
//	go run . -o OUT.ltx IN.db
//
// Flags are left at 0 (checksum tracking ENABLED) so the produced fixtures
// exercise the full CRC64-ISO + rolling post-apply checksum path.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/superfly/ltx"
)

const (
	sqliteHeaderString = "SQLite format 3\x00"
	sqliteHeaderSize   = 100
)

func main() {
	outPath := flag.String("o", "", "output .ltx path")
	// Fixed timestamp (ms) so regeneration is byte-reproducible. Override with -ts.
	ts := flag.Int64("ts", 1700000000000, "timestamp in unix millis")
	flag.Parse()
	if *outPath == "" || flag.NArg() != 1 {
		fmt.Println("usage: ltxgen -o OUT.ltx [-ts MILLIS] IN.db")
		os.Exit(2)
	}

	if err := run(flag.Arg(0), *outPath, *ts); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(dbPath, outPath string, ts int64) error {
	raw, err := os.ReadFile(dbPath)
	if err != nil {
		return fmt.Errorf("read db: %w", err)
	}
	if len(raw) < sqliteHeaderSize || !bytes.Equal(raw[:len(sqliteHeaderString)], []byte(sqliteHeaderString)) {
		return fmt.Errorf("not a SQLite database")
	}

	pageSize := uint32(binary.BigEndian.Uint16(raw[16:]))
	if pageSize == 1 {
		pageSize = 65536
	}
	pageN := binary.BigEndian.Uint32(raw[28:])
	if pageN == 0 {
		// Fall back to deriving from file length if header count is unset.
		pageN = uint32(uint64(len(raw)) / uint64(pageSize))
	}

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create out: %w", err)
	}
	defer out.Close()

	enc, err := ltx.NewEncoder(out)
	if err != nil {
		return fmt.Errorf("new encoder: %w", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version, // = 3 (the fix vs. the stale CLI)
		Flags:     0,           // checksum tracking enabled
		PageSize:  pageSize,
		Commit:    pageN,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: ts,
	}); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	rd := bytes.NewReader(raw)
	buf := make([]byte, pageSize)
	lockPgno := ltx.LockPgno(pageSize)
	var postApply ltx.Checksum
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		if _, err := io.ReadFull(rd, buf); err != nil {
			return fmt.Errorf("read page %d: %w", pgno, err)
		}
		if pgno == lockPgno {
			continue
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}
		postApply = ltx.ChecksumFlag | (postApply ^ ltx.ChecksumPage(pgno, buf))
	}

	enc.SetPostApplyChecksum(postApply)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder: %w", err)
	}

	fmt.Printf("wrote %s: page_size=%d commit=%d post_apply=%s ts=%s\n",
		outPath, pageSize, pageN, postApply, time.UnixMilli(ts).UTC().Format(time.RFC3339))
	return nil
}
