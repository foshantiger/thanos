package indexheader

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// Create block index version 2.
	b, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)

	bkt := inmem.NewBucket()
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, b.String())))

	t.Run("binary", func(t *testing.T) {
		fn := filepath.Join(tmpDir, b.String(), block.IndexHeaderFilename)
		testutil.Ok(t, WriteBinary(ctx, bkt, b, fn))

		br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, b)
		testutil.Ok(t, err)

		testutil.Equals(t, 1, br.version)
		testutil.Equals(t, 2, br.indexVersion)
		testutil.Equals(t, &BinaryTOC{Symbols: 0x8, PostingsOffsetTable: 0x20}, br.toc)
		testutil.Equals(t, 1, br.indexLastPostingEnd)
		testutil.Equals(t, 1, br.version)
		testutil.Equals(t, 1, br.version)
		testutil.Equals(t, 2, br.symbols.Size())
		testutil.Equals(t, 6, len(br.postings))
		testutil.Equals(t, 0, len(br.postingsV1))
		testutil.Equals(t, 2, len(br.nameSymbols))

		testReader(t, br)
	})

	t.Run("json", func(t *testing.T) {
		fn := filepath.Join(tmpDir, b.String(), block.IndexCacheFilename)
		testutil.Ok(t, WriteJSON(log.NewNopLogger(), filepath.Join(tmpDir, b.String(), "index"), fn))

		jr, err := NewJSONReader(ctx, log.NewNopLogger(), nil, tmpDir, b)
		testutil.Ok(t, err)

		testutil.Equals(t, 6, len(jr.symbols))
		testutil.Equals(t, 2, len(jr.lvals))
		testutil.Equals(t, 6, len(jr.postings))

		testReader(t, jr)
	})
}

func TestReaders_IndexFormatV1(t *testing.T) {
	_ = context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// Copy block index version 1 for backward compatibility.
	m, err := metadata.Read("./testdata/index_format_v1")
	testutil.Ok(t, err)
	testutil.Copy(t, filepath.Join(tmpDir, m.ULID.String()), "./testdata/index_format_v1")

	t.Run("binary", func(t *testing.T) {
	})

	t.Run("json", func(t *testing.T) {
	})

}

func testReader(t *testing.T, r Reader) {
	testutil.Equals(t, 2, r.IndexVersion())

	exp := []string{"1", "2", "3", "4", "a", "b"}
	for i := range exp {
		r, err := r.LookupSymbol(uint32(i))
		testutil.Ok(t, err)
		testutil.Equals(t, exp[i], r)
	}
	_, err := r.LookupSymbol(uint32(len(exp)))
	testutil.NotOk(t, err)

	vals, err := r.LabelValues("a")
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2", "3", "4"}, vals)

	vals, err = r.LabelValues("b")
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1"}, vals)

	vals, err = r.LabelValues("c")
	testutil.Ok(t, err)
	testutil.Equals(t, []string{}, vals)

	testutil.Equals(t, []string{"a", "b"}, r.LabelNames())

	ptr, err := r.PostingsOffset("a", "1")
	testutil.Ok(t, err)
	testutil.Equals(t, index.Range{Start: 200, End: 212}, ptr)

	ptr, err = r.PostingsOffset("a", "2")
	testutil.Ok(t, err)
	testutil.Equals(t, index.Range{Start: 220, End: 228}, ptr)

	ptr, err = r.PostingsOffset("b", "2")
	testutil.Ok(t, err)
	testutil.Equals(t, NotFoundRange, ptr)

	// TODO(bwplotka): Try to deduce postings from this!
}
