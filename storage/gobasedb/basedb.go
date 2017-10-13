/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package gobasedb

// Standard Library imports
import (
	"time"
	"path/filepath"
	"fmt"
	"os"
	"encoding/binary"
	"bytes"
)

// Blobserver imports
import (
	"github.com/maxymania/blobserver/storage"
	"github.com/maxymania/blobserver/istorage"
)

// Blobserver-related imports
import "github.com/valyala/bytebufferpool"
import "github.com/pierrec/lz4"

// Gobase-Imports
import (
	"github.com/maxymania/gobase/blocklist"
	"github.com/maxymania/gobase/skiplist"
	"github.com/maxymania/gobase/dataman"
	"github.com/maxymania/gobase/journal"
)

const dayTime = "20060102"

func split(bb []byte) (rb [][]byte) {
	rb = make([][]byte,0,1+(len(bb)/0x10000))
	for len(bb)>0x10000 {
		rb = append(rb,bb[:0x10000])
		bb = bb[0x10000:]
	}
	rb = append(rb,bb)
	return
}

func expand(buf []byte,i int) []byte {
	if cap(buf)<i { return make([]byte,i) }
	return buf[:i]
}

var blobPool bytebufferpool.Pool

func compress(blob []byte) *bytebufferpool.ByteBuffer {
	lblob := len(blob)
	i := lz4.CompressBlockBound(lblob)
	buf   := blobPool.Get()
	buf.B  = expand(buf.B,i+4)
	
	j,e := lz4.CompressBlock(blob,buf.B[4:],0)
	if e!=nil || j==0 {
		buf.B = buf.B[:4+lblob]
		copy(buf.B[4:],blob)
		lblob = 0
	} else {
		buf.B = buf.B[:4+j]
	}
	binary.BigEndian.PutUint32(buf.B[ :4],uint32(lblob))
	return buf
}

type baseStorage struct{
	dm        *dataman.DataManagerLocked
	dayIdx    int64
	trackOff  int64 // Storage tracking record.
	blockList *blocklist.BLManager // FreeBlockList
	minTime   time.Time
	freed     int64
	maxSpace  int64
}

func (s *baseStorage) persistFreed() error {
	var i64 blocklist.Int64
	i64.SetInt64(s.freed)
	_,err := s.dm.RollbackFile().WriteAt(i64[:],s.trackOff)
	return err
}
func (s *baseStorage) updateFreed(unused dataman.DataManager,amount int64) error {
	s.freed += amount
	return s.persistFreed()
}

func (s *baseStorage) getHead(categ []byte) (int64,error) {
	slm := skiplist.NodeMaster.Open(s.dm,false)
	defer slm.Flush()
	idx,ok,err := skiplist.Lookup(slm,s.dayIdx,categ)
	if err!=nil { return 0,err }
	
	if ok { return idx,nil }
	
	lh,err := blocklist.NewListHead(s.dm)
	if err!=nil { return 0,err }
	
	err = skiplist.InsertionAlgorithmV1(slm,s.dayIdx,categ, lh)
	if err!=nil { return 0,err }
	
	return lh,nil
}
func (s *baseStorage) allocStorage(categ []byte, bl int) ([]blocklist.BufAddr,error) {
	s.dm.Lock(); defer s.dm.Unlock()
	lh,err := s.getHead(categ) // Get the list-head
	if err!=nil { return nil,err }
	
	lst,err := blocklist.Allocate(s.dm,bl)
	if err!=nil { return nil,err }
	
	// Add the Chain to the list-header and Chainify them.
	blocklist.Chainify(s.dm,lst,lh)
	
	// Track freed and Reallocated Storage.
	for _,elem := range lst {
		s.freed -= int64(elem.Len+16)
		if s.freed<0 { s.freed = 0; break }
	}
	s.persistFreed() // Persist the Freed-Value - ignore any error.
	
	err = s.dm.Commit()
	
	if err!=nil { return nil,err }
	
	return lst,nil
}
func (s *baseStorage) store(categ, bb []byte) (int64,error) {
	lst,err := s.allocStorage(categ,len(bb))
	if err!=nil { return 0,err }
	if len(lst)==0 { return 0,fmt.Errorf("Empty List") }
	
	last := len(lst)-1
	
	df := s.dm.DirectFile()
	
	for i,elem := range lst {
		chunk := len(bb)
		if chunk>elem.Len { chunk = elem.Len }
		err = blocklist.SetExtendedLen(df,elem.Off,chunk,i==last)
		if err!=nil { return 0,err }
		_,err = df.WriteAt(bb[:chunk],elem.Off+16)
		if err!=nil { return 0,err }
		bb = bb[chunk:]
	}
	
	return lst[0].Off,nil
}
func (s *baseStorage) load(off int64) (buf *bytebufferpool.ByteBuffer) {
	var dbgbuf [16]byte
	buf  = blobPool.Get()
	df  := s.dm.DirectFile()
	
	for {
		lng,eol,err := blocklist.GetExtendedLen(df,off)
		df.ReadAt(dbgbuf[:],off)
		if err!=nil { goto cut }
		oln := len(buf.B)
		buf.B = expand(buf.B,oln+lng)
		_,err = df.ReadAt(buf.B[oln:],off+16)
		if err!=nil { goto cut }
		
		if eol { break }
		
		off,err = blocklist.GetNext(df,off)
		if err!=nil { goto cut }
		if off==0 { break } // Just in case/ Safety first.
	}
	
	return buf
	
	cut:
	blobPool.Put(buf)
	return nil
}

func (s *baseStorage) StoreBlob(blob []byte, t time.Time) ([]byte, bool) {
	{
		// Don't pass the time-barrier.
		ot := s.minTime
		if ot.After(t) { return nil,false }
	}
	var key [8]byte
	tk := t.UTC().AppendFormat(key[:0],dayTime)
	buf := compress(blob)
	defer blobPool.Put(buf)
	k,err := s.store(tk,buf.B)
	if err!=nil { return nil,false }
	b := make([]byte,8)
	binary.BigEndian.PutUint64(b,uint64(k))
	return b,true
}

func (s *baseStorage) LoadBlob(key []byte, target *bytebufferpool.ByteBuffer) (lz4l int, ok bool) {
	if len(key)!=8 { return }
	off := int64(binary.BigEndian.Uint64(key))
	buf := s.load(off)
	if buf==nil { return }
	defer blobPool.Put(buf)
	if len(buf.B)<4 { return }
	lz4l = int(binary.BigEndian.Uint32(buf.B))
	target.Set(buf.B[4:])
	ok = true
	return
}
func (s *baseStorage) obtain(categ []byte) func(dm dataman.DataManager)(int64,error) {
	return func(dm dataman.DataManager)(int64,error) {
		slm := skiplist.NodeMaster.Open(s.dm,false)
		value,ok,err := skiplist.ConsumeFirstIfLowerOrEqual(slm, s.dayIdx, categ)
		if err!=nil { return 0,err }
		if !ok { value = 0 }
		return value,nil
	}
}

func (s *baseStorage) Expire(t time.Time) {
	var key [8]byte
	s.minTime = t
	tk := t.UTC().AppendFormat(key[:0],dayTime)
	obtain := s.obtain(tk)
	// Redo this, until all daynodes earlier than tk are deleted.
	for {
		consumed,err := s.blockList.AppendNodeAndConsume(obtain)
		if err!=nil { break }
		if !consumed { break }
	}
}
func (s *baseStorage) FreeStorage() int64 {
	fspace := s.maxSpace
	stat,err := s.dm.DirectFile().Stat()
	if err!=nil { return 0 }
	fspace -= stat.Size()
	if fspace<0 { fspace = 0 }
	fspace += s.freed
	return fspace
}


type masterRecord struct{
	DayIndex      int64
	TrackRecord   int64
	FreeBlockList int64
	
	Padding       int64
}
func (m *masterRecord) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,*m)
	return buf.Bytes()
}
func (m *masterRecord) SetBytes(b []byte) {
	binary.Read(bytes.NewReader(b),binary.BigEndian,m)
}

const journalSize = (1<<24)

func open_baseStorage(fn string, maxSpace int64) (*baseStorage,error) {
	var i64 blocklist.Int64
	var zero8 [8]byte
	var zero16 [16]byte
	for i := range zero8 { zero8[i] = 0 }
	for i := range zero16 { zero16[i] = 0 }
	
	f,e := os.OpenFile(fn,os.O_CREATE|os.O_RDWR,0600)
	if e!=nil { return nil,e }
	
	stat,e := f.Stat()
	if e!=nil { f.Close(); return nil,e }
	
	isFresh := stat.Size()<=journalSize
	
	ipw := journal.NewInplaceWAL_File(&journal.OffsetFile{f, 8}, journalSize-8)
	osf := &journal.OffsetFile{f, journalSize}
	
	jf,err := journal.NewJournalDataManager(osf, ipw)
	if err!=nil { return nil,err }
	
	st := new(baseStorage)
	mr := new(masterRecord)
	
	if isFresh {
		
		mri,err := jf.Alloc(32) // masterRecord
		if err!=nil { return nil,err }
		i64.SetInt64(mri)
		
		idx,err := skiplist.NodeMaster.Open(jf,false).Set(new(skiplist.Node))
		if err!=nil { return nil,err }
		mr.DayIndex = idx
		
		idx,err = jf.Alloc(8)
		if err!=nil { return nil,err }
		mr.TrackRecord = idx
		_,err = jf.RollbackFile().WriteAt(zero8[:],idx)
		if err!=nil { return nil,err }
		
		idx,err = jf.Alloc(16)
		if err!=nil { return nil,err }
		mr.FreeBlockList = idx
		_,err = jf.RollbackFile().WriteAt(zero16[:],idx)
		if err!=nil { return nil,err }
		
		
		_,err = jf.RollbackFile().WriteAt(mr.Bytes(),mri)
		if err!=nil { return nil,err }
		
		// Write Header.
		_,err = f.WriteAt(i64[:],0)
		if err!=nil { return nil,err }
		
		err = jf.Commit()
		if err!=nil { return nil,err }
	} else {
		buf := make([]byte,32)
		_,err = f.ReadAt(i64[:],0)
		if err!=nil { return nil,err }
		_,err = jf.DirectFile().ReadAt(buf,i64.Int64())
		if err!=nil { return nil,err }
		
		mr.SetBytes(buf)
	}
	
	_,err = jf.DirectFile().ReadAt(i64[:],mr.TrackRecord)
	if err!=nil { return nil,err }
	
	st.dm = &dataman.DataManagerLocked{DataManager:jf}
	st.dayIdx    = mr.DayIndex
	st.trackOff  = mr.TrackRecord
	st.blockList = &blocklist.BLManager{ DM:st.dm, Off: mr.FreeBlockList }
	st.freed     = i64.Int64()
	st.maxSpace  = maxSpace
	
	return st,nil
}

func init() {
	storage.Backends["basedb"] = gobasedbLoader
}

func gobasedbLoader(path string, cfg *storage.StorageConfig) (string,istorage.Storage,error) {
	uuid,err := storage.GetOrCreateUUID(path)
	if err!=nil { return "",nil,err }
	bs,err := open_baseStorage(filepath.Join(path,"gobasedb.dat"),cfg.Capacity.Int64())
	if err!=nil { return "",nil,err }
	
	return string(uuid[:]),bs,nil
}

