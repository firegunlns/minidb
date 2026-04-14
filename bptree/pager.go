package bptree

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

const (
	defaultSlotSize             = 32768 // 32KB
	pagerHeaderSize       int64 = 4096
	magicValue      uint32      = 0x42505452 // "BPTR"
	currentVersion  uint32      = 2
)

var ErrCorrupted = errors.New("bptree: corrupted file")

// fileHeader is stored at the beginning of the data file.
// Layout (32 bytes):
//
//	[0:4]   Magic
//	[4:8]   Version
//	[8:12]  Order
//	[12:16] SlotSize
//	[16:24] RootPageID
//	[24:32] PageCount
type fileHeader struct {
	Magic      uint32
	Version    uint32
	Order      uint32
	SlotSize   uint32
	RootPageID int64
	PageCount  int64
}

// Pager manages fixed-size page slots in a single data file.
// Page N is stored at file offset: pagerHeaderSize + N * slotSize.
// Each slot layout: [4 bytes dataSize][dataSize bytes data][zero-padding].
type Pager struct {
	mu        sync.Mutex
	file      *os.File
	slotSize  int64
	pageCount int64
}

// NewPager opens or creates a pager backed by filePath.
// If slotSize <= 0, defaultSlotSize is used.
// For existing files the slot size stored in the header takes precedence.
func NewPager(filePath string, slotSize int64) (*Pager, error) {
	if slotSize <= 0 {
		slotSize = defaultSlotSize
	}

	p := &Pager{slotSize: slotSize}

	info, err := os.Stat(filePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if os.IsNotExist(err) || info.Size() == 0 {
		f, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		if err := f.Truncate(pagerHeaderSize); err != nil {
			f.Close()
			return nil, err
		}
		p.file = f
		if err := p.writeFileHeader(fileHeader{
			Magic:    magicValue,
			Version:  currentVersion,
			SlotSize: uint32(slotSize),
		}); err != nil {
			f.Close()
			return nil, err
		}
		return p, nil
	}

	// Existing file — read header.
	f, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	p.file = f

	h, err := p.readFileHeader()
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.Magic != magicValue || h.Version > currentVersion {
		f.Close()
		return nil, ErrCorrupted
	}
	p.slotSize = int64(h.SlotSize)
	p.pageCount = h.PageCount
	return p, nil
}

// Allocate returns the next available page ID.
func (p *Pager) Allocate() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.pageCount
	p.pageCount++
	return id, nil
}

// Read loads the data stored in the given page.
func (p *Pager) Read(pageID int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readPage(pageID)
}

func (p *Pager) readPage(pageID int64) ([]byte, error) {
	offset := pagerHeaderSize + pageID*p.slotSize
	sizeBuf := make([]byte, 4)
	if _, err := p.file.ReadAt(sizeBuf, offset); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(sizeBuf)
	if size == 0 || int64(size) > p.slotSize-4 {
		return nil, errors.New("bptree: invalid page data")
	}
	data := make([]byte, size)
	if _, err := p.file.ReadAt(data, offset+4); err != nil {
		return nil, err
	}
	return data, nil
}

// Write stores data into the slot for the given page.
func (p *Pager) Write(pageID int64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writePage(pageID, data)
}

func (p *Pager) writePage(pageID int64, data []byte) error {
	if int64(len(data))+4 > p.slotSize {
		return errors.New("bptree: node data exceeds page slot size")
	}
	offset := pagerHeaderSize + pageID*p.slotSize

	// Ensure file is large enough.
	end := offset + p.slotSize
	if fi, err := p.file.Stat(); err == nil && fi.Size() < end {
		p.file.Truncate(end)
	}

	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	if _, err := p.file.WriteAt(buf, offset); err != nil {
		return err
	}
	return nil
}

// WriteHeader persists tree metadata into the file header.
func (p *Pager) WriteHeader(rootID int64, order int, version uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeFileHeader(fileHeader{
		Magic:      magicValue,
		Version:    version,
		Order:      uint32(order),
		SlotSize:   uint32(p.slotSize),
		RootPageID: rootID,
		PageCount:  p.pageCount,
	})
}

func (p *Pager) writeFileHeader(h fileHeader) error {
	buf := make([]byte, pagerHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:], h.Magic)
	binary.LittleEndian.PutUint32(buf[4:], h.Version)
	binary.LittleEndian.PutUint32(buf[8:], h.Order)
	binary.LittleEndian.PutUint32(buf[12:], h.SlotSize)
	binary.LittleEndian.PutUint64(buf[16:], uint64(h.RootPageID))
	binary.LittleEndian.PutUint64(buf[24:], uint64(h.PageCount))
	_, err := p.file.WriteAt(buf[:32], 0)
	return err
}

// ReadHeader loads the file header.
func (p *Pager) ReadHeader() (fileHeader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readFileHeader()
}

func (p *Pager) readFileHeader() (fileHeader, error) {
	buf := make([]byte, 32)
	if _, err := p.file.ReadAt(buf, 0); err != nil {
		return fileHeader{}, err
	}
	var h fileHeader
	h.Magic = binary.LittleEndian.Uint32(buf[0:])
	h.Version = binary.LittleEndian.Uint32(buf[4:])
	h.Order = binary.LittleEndian.Uint32(buf[8:])
	h.SlotSize = binary.LittleEndian.Uint32(buf[12:])
	h.RootPageID = int64(binary.LittleEndian.Uint64(buf[16:]))
	h.PageCount = int64(binary.LittleEndian.Uint64(buf[24:]))
	return h, nil
}

// Sync flushes pending writes to the underlying storage.
func (p *Pager) Sync() error {
	return p.file.Sync()
}

// Close syncs and closes the underlying file.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		p.file.Sync()
		return p.file.Close()
	}
	return nil
}
