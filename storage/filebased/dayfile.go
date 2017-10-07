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
import "time"
import "encoding/binary"
import "io/ioutil"
import "os"
import "path/filepath"
import "github.com/maxymania/blobserver/storage"
import "github.com/maxymania/blobserver/istorage"

func isDayfile(name string) bool {
	if len(name)!=8 { return false }
	for _,b := range []byte(name) {
		if b<'0' || '9'<b { return false }
	}
	return true
}

const dayFile_Fmt = "20060102"
const dayFile_Seconds = 60*60*24
type dayFile struct{
	ao *aoFolder
	ex time.Time
	wf aoWriteFunc
	// --------------------------------------
	spaceTrack   *sizeTrack
	maxSpace     int64
	folder       string
}

func (d *dayFile) StoreBlob(blob []byte, t time.Time) ([]byte,bool) {
	var buf [32]byte // (10+10+5) = 25, 32 for alignment
	t = t.UTC().Truncate(time.Hour*24)
	if d.ex.After(t) { return nil,false } // Don't reopen old dayfiles
	
	un := t.Unix()/dayFile_Seconds
	df := t.Format(dayFile_Fmt)
	offset,lng,ok := d.ao.getFile(df).writeBlob(blob,d.wf)
	if !ok { return nil,false }
	d.spaceTrack.addFile(df,int64(lng))
	i := binary.PutVarint(buf[ :],un)
	i += binary.PutVarint(buf[i:],offset)
	i += binary.PutVarint(buf[i:],int64(lng))
	return append(make([]byte,0,i),buf[:i]...),true
}
func (d *dayFile) LoadBlob(key []byte,target *bytebufferpool.ByteBuffer) (lz4l int,ok bool) {
	daynum,i := binary.Varint(key) ; key = key[i:]
	offset,i := binary.Varint(key) ; key = key[i:]
	lng   ,_ := binary.Varint(key)
	t := time.Unix(daynum*dayFile_Seconds,0).UTC()
	if d.ex.After(t) { return }
	df := t.Format(dayFile_Fmt)
	return d.ao.getFile(df).readBlob(offset,int(lng),target)
}
func (d *dayFile) Expire(t time.Time) {
	if !t.After(d.ex) { return }
	df := t.Format(dayFile_Fmt)
	fis,_ := ioutil.ReadDir(d.folder)
	for _,fi := range fis {
		name := fi.Name()
		if !isDayfile(name) { continue }
		if df<name { continue }
		d.ao.getFile(name).disable()
		os.Remove(filepath.Join(d.folder,name))
		d.spaceTrack.setFile(name,0)
	}
}
func (d *dayFile) FreeStorage() int64 {
	return d.maxSpace-d.spaceTrack.count
}
//
func dayfileLoader(path string, cfg *storage.StorageConfig) (string,istorage.Storage,error) {
	uuid,err := storage.GetOrCreateUUID(path)
	if err!=nil { return "",nil,err }
	d           := &dayFile{}
	d.ao         = aoFolderNew(path,cfg.MaxOpenFiles)
	d.wf         = getAoWriteFunc(cfg)
	d.spaceTrack = sizeTrackNew()
	d.maxSpace   = cfg.Capacity.Int64()
	d.folder     = path
	fis,_ := ioutil.ReadDir(path)
	for _,fi := range fis {
		name := fi.Name()
		if !isDayfile(name) { continue }
		d.spaceTrack.setFile(name,fi.Size())
	}
	return string(uuid[:]),d,nil
}

