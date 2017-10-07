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


package binascii

func bcadd(data uint64,siz uint, word uint64, wl uint) (uint64,uint) {
	data |= word << siz
	siz += wl
	return data,siz
}
func bcpeek(data uint64,lng uint) (uint64) {
	return data&((1<<lng)-1)
}
func bcpull(data uint64,siz uint,wl uint) (uint64,uint) {
	data>>=wl
	siz-=wl
	return data,siz
}

func highLe190(data uint64) bool {
	b := data&0x7f
	return ('0'<=b && b<='9') || ('a'<=b && b<='z') || ('A'<=b && b<='Z')
}
func isLe190(b byte) bool {
	return ('0'<=b && b<='9') || ('a'<=b && b<='z') || ('A'<=b && b<='Z') || (0x7f<b)
}

func EncodeLe190(data []byte, buffer []byte) []byte {
	var dreg uint64
	var dsiz uint
	for _,b := range data {
		dreg,dsiz = bcadd(dreg,dsiz,uint64(b),8)
		for 8 <= dsiz {
			if highLe190(dreg) {
				buffer = append(buffer,byte(bcpeek(dreg,8)))
				dreg,dsiz = bcpull(dreg,dsiz,8)
			} else {
				buffer = append(buffer,byte(bcpeek(dreg,7))|0x80)
				dreg,dsiz = bcpull(dreg,dsiz,7)
			}
		}
	}
	if 0 < dsiz {
		if highLe190(dreg) {
			buffer = append(buffer,byte(bcpeek(dreg,8)))
		} else {
			buffer = append(buffer,byte(bcpeek(dreg,7))|0x80)
		}
	}
	return buffer
}

func DecodeLe190(encoded []byte, buffer []byte) ([]byte,error) {
	var dreg uint64
	var dsiz uint
	for _,b := range encoded {
		if !isLe190(b) { continue }
		word := uint64(b)
		if highLe190(word) {
			dreg,dsiz = bcadd(dreg,dsiz,word&0xff,8)
		} else {
			dreg,dsiz = bcadd(dreg,dsiz,word&0x7f,7)
		}
		if 8 <= dsiz {
			buffer = append(buffer,byte(bcpeek(dreg,8)))
			dreg,dsiz = bcpull(dreg,dsiz,8)
		}
	}
	return buffer,nil
}

func IntToLe190(I uint64, buffer []byte) []byte {
	var dreg uint64
	var dsiz uint
	dreg = I
	dsiz = 64
	for 8 <= dsiz {
		if highLe190(dreg) {
			buffer = append(buffer,byte(bcpeek(dreg,8)))
			dreg,dsiz = bcpull(dreg,dsiz,8)
		} else {
			buffer = append(buffer,byte(bcpeek(dreg,7))|0x80)
			dreg,dsiz = bcpull(dreg,dsiz,7)
		}
		if dreg==0 { return buffer }
	}
	if 0 < dsiz {
		if highLe190(dreg) {
			buffer = append(buffer,byte(bcpeek(dreg,8)))
		} else {
			buffer = append(buffer,byte(bcpeek(dreg,7))|0x80)
		}
	}
	return buffer
}

func IntFromLe190(encoded []byte) (uint64) {
	var dreg uint64
	var dsiz uint
	for _,b := range encoded {
		if !isLe190(b) { continue }
		word := uint64(b)
		if highLe190(word) {
			dreg,dsiz = bcadd(dreg,dsiz,word&0xff,8)
		} else {
			dreg,dsiz = bcadd(dreg,dsiz,word&0x7f,7)
		}
	}
	return dreg
}

func Signed(u uint64) int64 {
	if (u&1)==1 {
		return -int64(u>>1)
	}
	return int64(u>>1)
}

func Unsigned(i int64) uint64 {
	b := i<0
	if b { i = -i }
	u := uint64(i)<<1
	if b { u|=1 }
	return u
}

