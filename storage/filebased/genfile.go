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


package filebased

import "github.com/valyala/bytebufferpool"
import "github.com/byte-mug/golibs/reslink"
import "os"
import "sync"
import "sync/atomic"
import "path/filepath"
import "github.com/maxymania/blobserver/storage"

func expand(buf []byte,i int) []byte {
	if cap(buf)<i { return make([]byte,i) }
	return buf[:i]
}

var blobPool bytebufferpool.Pool

type genericFile struct{
	*os.File
	FileName string
}
func (g *genericFile) Open() error {
	f,e := os.OpenFile(g.FileName,os.O_RDWR|os.O_CREATE,0600)
	if e!=nil { return e }
	g.File = f
	return nil
}
func (g *genericFile) Close() error {
	if g.File==nil { return nil }
	return g.File.Close()
}

type aoFolder struct{
	total  *reslink.ResourceList
	prefix string
	files  map[string]*aoFile
	mutex  sync.Mutex
}
func aoFolderNew(p string,max int) *aoFolder {
	a := new(aoFolder)
	a.total  = reslink.NewResourceList(max)
	a.prefix = p
	a.files  = make(map[string]*aoFile)
	return a
}
func (a *aoFolder) getFile(name string) *aoFile {
	a.mutex.Lock(); defer a.mutex.Unlock()
	f,ok := a.files[name]
	if ok { return f }
	f = aoFileNew(a.total,filepath.Join(a.prefix,name))
	a.files[name] = f
	return f
}

type aoFile struct{
	file  *genericFile
	elem  *reslink.ResourceElement
	total *reslink.ResourceList
	mutex sync.Mutex
	count *int64
}
func aoFileNew(total *reslink.ResourceList,f string) *aoFile {
	file := &genericFile{nil,f}
	a := new(aoFile)
	a.file  = file
	a.elem  = reslink.NewResourceElement(file)
	a.total = total
	a.count = new(int64)
	//*a.count = -1
	return a
}

type aoWriteFunc func(a *aoFile, b *bytebufferpool.ByteBuffer) (int64,int,bool)

func getAoWriteFunc(cfg *storage.StorageConfig) (a aoWriteFunc){
	a = aofAppendDirect
	for _,cg := range cfg.Options {
		if cg=="pwrite" {
		a = aofAppendWriteAt
		}
	}
	return
}


func (a *aoFile) writeBlob(blob []byte,f aoWriteFunc) (int64,int,bool) {
	buf := compress(blob)
	return f(a,buf)
}
func (a *aoFile) disable() { a.total.Disable(a.elem) }
func (a *aoFile) readBlob(offset int64, lng int,targ *bytebufferpool.ByteBuffer) (lz4l int,ok bool) {
	a.elem.Incr(); defer a.elem.Decr()
	if err := a.total.Open(a.elem) ; err!=nil { return }
	return unpacked(a.file,offset,lng,targ)
}


func aofAppendDirect(a *aoFile, b *bytebufferpool.ByteBuffer) (int64,int,bool) {
	defer blobPool.Put(b)
	a.elem.Incr(); defer a.elem.Decr()
	if err := a.total.Open(a.elem) ; err!=nil { return 0,0,false }
	a.mutex.Lock(); defer a.mutex.Unlock()
	pos,err := a.file.Seek(0,2)
	if err!=nil { return 0,0,false }
	_,err = a.file.Write(b.B)
	if err!=nil { return 0,0,false }
	return pos,b.Len(),true
}

func aofAppendWriteAt(a *aoFile, b *bytebufferpool.ByteBuffer) (int64,int,bool) {
	defer blobPool.Put(b)
	a.elem.Incr(); defer a.elem.Decr()
	if err := a.total.Open(a.elem) ; err!=nil { return 0,0,false }
	
	if pos := atomic.LoadInt64(a.count) ; pos<=0 {
		a.mutex.Lock()
		npos,err := a.file.Seek(0,2)
		a.mutex.Unlock()
		atomic.CompareAndSwapInt64(a.count,pos,npos)
		if err!=nil { return 0,0,false }
	}
	
	l := len(b.B)
	neof := atomic.AddInt64(a.count,int64(l))
	beg  := neof-int64(l)
	
	_,err := a.file.WriteAt(b.B,beg)
	if err!=nil {
		// Revert increment, if possible
		atomic.CompareAndSwapInt64(a.count,neof,beg)
		return 0,0,false
	}
	return beg,b.Len(),true
}


type sizeTrack struct{
	mutex sync.Mutex
	files map[string]int64
	count int64
}
func sizeTrackNew() *sizeTrack {
	return &sizeTrack{files: make(map[string]int64) }
}

func (s *sizeTrack) setFile(n string,size int64) {
	s.mutex.Lock(); defer s.mutex.Unlock()
	diff := size-s.files[n]
	if size!=0 {
		s.files[n] = size
	} else {
		delete(s.files,n)
	}
	s.count += diff
}
func (s *sizeTrack) addFile(n string,diff int64) {
	s.mutex.Lock(); defer s.mutex.Unlock()
	s.files[n] = s.files[n] + diff
	s.count += diff
}


