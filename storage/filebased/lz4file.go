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
import "github.com/pierrec/lz4"
import "encoding/binary"
import "io"

func compress(blob []byte) *bytebufferpool.ByteBuffer {
	lblob := len(blob)
	i := lz4.CompressBlockBound(lblob)
	buf   := blobPool.Get()
	buf.B  = expand(buf.B,i+8)
	
	j,e := lz4.CompressBlock(blob,buf.B[8:],0)
	if e!=nil || j==0 {
		buf.B = buf.B[:8+lblob]
		copy(buf.B[8:],blob)
		lblob = 0
	} else {
		buf.B = buf.B[:8+j]
	}
	binary.BigEndian.PutUint32(buf.B[ :4],uint32(lblob))
	binary.BigEndian.PutUint32(buf.B[4:8],uint32(len(buf.B))-8)
	return buf
}
func unpacked(rat io.ReaderAt,offset int64, lng int, targ *bytebufferpool.ByteBuffer) (lz4l int,ok bool) {
	var buf [8]byte
	n,err := rat.ReadAt(buf[:],offset)
	if n!=16 && err!=nil { return }
	
	lz4l = int(binary.BigEndian.Uint32(buf[ :4]))
	j := int(binary.BigEndian.Uint32(buf[4:8]))
	if (j+8)>lng { return }
	targ.B  = expand(targ.B,j)
	n,err = rat.ReadAt(targ.B,offset+8)
	if n!=j && err!=nil { return }
	ok = true
	return
}



