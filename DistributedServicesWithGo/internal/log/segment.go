// The segment wraps the index and store types to coordinate operations across
// the two. For example, when the log appends a record to the active segment,
// the segment needs to write the data to its store and add a new entry in the
// index. Similarly for reads, the segment needs to look up the entry from the
// index and then fetch the data from the store.

package log

import (
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"
	api "utwoo.com/DistributedServicesWithGo/api/v1"
)

// Our segment needs to call its store and index files, so we keep pointers to
// those in the first two fields. We need the next and base offsets to know what
// offset to append new records under and to calculate the relative offsets for
// the index entries. And we put the config on the segment so we can compare
// the store file and index sizes to the configured limits, which lets us know
// when the segment is maxed out.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// The log calls newSegment() when it needs to add a new segment, such as when
// the current active segment hits its max size. We open the store and index
// files and pass the os.O_CREATE file mode flag as an argument to os.OpenFile() to
// create the files if they don’t exist yet. When we create the store file, we pass
// the os.O_APPEND flag to make the operating system append to the file when
// writing. Then we create our index and store with these files. Finally, we set
// the segment’s next offset to prepare for the next appended record. If the index
// is empty, then the next record appended to the segment would be the first
// record and its offset would be the segment’s base offset. If the index has at
// least one entry, then that means the offset of the next record written should
// take the offset at the end of the segment, which we get by adding 1 to the
// base offset and relative offset. Our segment is ready to write to and read from
// the log—once we’ve written those methods!
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record to the segment and returns the newly appended
// record’s offset. The log returns the offset to the API response. The segment
// appends a record in a two-step process: it appends the data to the store and
// then adds an index entry. Since index offsets are relative to the base offset,
// we subtract the segment’s next offset from its base offset (which are both
// absolute offsets) to get the entry’s relative offset in the segment. We then
// increment the next offset to prep for a future append call.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read(off uint64) returns the record for the given offset. Similar to writes, to read
// a record the segment must first translate the absolute index into a relative
// offset and get the associated index entry. Once it has the index entry, the
// segment can go straight to the record’s position in the store and read the
// proper amount of data.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size, either by
// writing too much to the store or the index. If you wrote a small number of
// long logs, then you’d hit the segment bytes limit; if you wrote a lot of small
// logs, then you’d hit the index bytes limit. The log uses this method to know
// it needs to create a new segment.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple(j uint64, k uint64) returns the nearest and lesser multiple of k in j ,
// for example nearestMultiple(9, 4) == 8 . We take the lesser multiple to make sure
// we stay under the user’s disk capacity.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
