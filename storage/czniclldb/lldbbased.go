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


package czniclldb

import "github.com/cznic/lldb"
import "bytes"
import "github.com/valyala/bytebufferpool"
import "encoding/binary"
import "sync"
import "time"
import "github.com/pierrec/lz4"
import "path/filepath"
import "fmt"
import "github.com/maxymania/blobserver/storage"
import "github.com/maxymania/blobserver/istorage"
import "os"

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
	
	j,e := lz4.CompressBlock(blob,buf.B[8:],0)
	if e!=nil || j==0 {
		buf.B = buf.B[:8+lblob]
		copy(buf.B[8:],blob)
		lblob = 0
	} else {
		buf.B = buf.B[:8+j]
	}
	binary.BigEndian.PutUint32(buf.B[ :4],uint32(lblob))
	return buf
}


const (
	hasNext uint8 = 1<<iota
	hasMore // Next Packet belongs to this stream.
)

type header struct{
	Next  int64
	Flags uint8
}

type llstorage struct{
	buf  bytes.Buffer
	all  *lldb.Allocator
	tree *lldb.BTree
	mutx sync.RWMutex
}
func (s *llstorage) store(categ, bb []byte) (int64,error) {
	s.mutx.Lock(); defer s.mutx.Unlock()
	var myBuf [9]byte
	rb := split(bb)
	h := header{}
	obj,err := s.tree.Get(myBuf[:], categ)
	if err!=nil { return 0,err }
	if len(obj)==9 {
		h.Next  = int64(binary.BigEndian.Uint64(obj))
		h.Flags = obj[8] & ^hasMore
	}
	// Create the linked list backwards.
	for i := len(rb) ; i>0 ; {
		i--
		s.buf.Reset()
		binary.Write(&s.buf,binary.BigEndian,h)
		s.buf.Write(rb[i])
		handle,err := s.all.Alloc(s.buf.Bytes())
		if err!=nil { return 0,err }
		h.Next = handle
		h.Flags = hasNext|hasMore
	}
	
	binary.BigEndian.PutUint64(myBuf[:8],uint64(h.Next))
	myBuf[8] = h.Flags
	s.tree.Set(categ,myBuf[:])
	return h.Next,nil // Return the head of the list.
}
const dayTime = "20060102"

func (s *llstorage) StoreBlob(blob []byte, t time.Time) ([]byte, bool) {
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
func (s *llstorage) LoadBlob(key []byte, target *bytebufferpool.ByteBuffer) (lz4l int, ok bool) {
	if len(key)!=8 { return }
	handle := int64(binary.BigEndian.Uint64(key))
	obj,err := s.all.Get(nil,handle)
	if err!=nil || len(obj)<13 { return } // 9+4 = 13
	h := header{}
	h.Next = int64(binary.BigEndian.Uint64(obj))
	h.Flags = obj[8]
	
	lz4l = int(binary.BigEndian.Uint32(obj[9:]))
	target.Write(obj[13:])
	for (h.Flags & (hasNext|hasMore))==(hasNext|hasMore) {
		obj,err = s.all.Get(nil,h.Next)
		if err!=nil || len(obj)<9 { return }
		h.Next = int64(binary.BigEndian.Uint64(obj))
		h.Flags = obj[8]
		target.Write(obj[9:])
	}
	ok = true
	return
}
func (s *llstorage) Expire(t time.Time) {}
func (s *llstorage) FreeStorage() int64 {
	return 0
}


func init() {
	storage.Backends["clldb"] = clldbLoader
}

func clldbLoader(path string, cfg *storage.StorageConfig) (string,istorage.Storage,error) {
	uuid,err := storage.GetOrCreateUUID(path)
	if err!=nil { return "",nil,err }
	f,err := os.OpenFile(filepath.Join(path,"clldb.dat"),os.O_CREATE|os.O_RDWR,0600)
	if err!=nil { return "",nil,err }
	fileLength,err := f.Seek(0,2)
	if err!=nil { return "",nil,err }
	all,err := lldb.NewAllocator(lldb.NewSimpleFileFiler(f),&lldb.Options{})
	if err!=nil { return "",nil,err }
	
	s := new(llstorage)
	s.all  = all
	if fileLength==0 {
		bt,h,err := lldb.CreateBTree(s.all,bytes.Compare)
		if err!=nil { return "",nil,err }
		if h!=1 { return "",nil,fmt.Errorf("Invalid Handle %v !=1",h) }
		s.tree = bt
	} else {
		bt,err := lldb.OpenBTree(s.all,bytes.Compare,1)
		if err!=nil { return "",nil,err }
		s.tree = bt
	}
	
	//d.maxSpace   = cfg.Capacity.Int64()
	
	return string(uuid[:]),s,nil
}
