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


package client

import "github.com/maxymania/blobserver/binascii"
import "github.com/byte-mug/gocom/notrest"
import "github.com/pierrec/lz4"
import "time"

func realloc(buf []byte, i int) []byte {
	if cap(buf)<i { return make([]byte,i) }
	return buf[:i]
}

func decint(str []byte) (i int) {
	for _,b := range str {
		if b<'0' || '9'<b { continue }
		i = (i*10) + int(b-'0')
	}
	return
}

type Client struct{
	Client *notrest.Client
	tempbuf [128]byte
}

func (c *Client) PostBlob(blob []byte, t time.Time, nbuf,ibuf []byte) (
			node []byte,ID []byte,ok bool,err error) {
	req  := notrest.AckquireRequest ()
	resp := notrest.AckquireResponse()
	defer notrest.ReleaseRequest (req )
	defer notrest.ReleaseResponse(resp)
	
	req.SetMethodStr("post")
	{
		path := append(c.tempbuf[:0],"/blobs/"...)
		path  = binascii.IntToLe190(binascii.Unsigned(t.Unix()),path)
		req.SetPath(path)
	}
	req.Body().Set(blob)
	err = c.Client.Do(req,resp)
	if err!=nil || resp.Code()!=204 { return }
	node,_ = binascii.DecodeLe190(resp.GetHeaderK("node"),nbuf)
	ID  ,_ = binascii.DecodeLe190(resp.GetHeaderK("id"),ibuf)
	ok     = len(ID)>0
	return
}
func (c *Client) GetBlob(node []byte,ID []byte,blobbuf []byte) (blob []byte,ok bool,err error) {
	req  := notrest.AckquireRequest ()
	resp := notrest.AckquireResponse()
	defer notrest.ReleaseRequest (req )
	defer notrest.ReleaseResponse(resp)
	
	req.SetMethodStr("get")
	{
		path := append(c.tempbuf[:0],"/blobs/"...)
		path  = binascii.EncodeLe190(node,path)
		path  = append(path,'/')
		path  = binascii.EncodeLe190(ID,path)
		req.SetPath(path)
	}
	err = c.Client.Do(req,resp)
	if err!=nil || resp.Code()!=200 { return }
	
	if decomp := decint(resp.GetHeaderK("lz4-size")) ; 0 < decomp {
		buf := realloc(blobbuf,decomp)
		decomp,err = lz4.UncompressBlock(resp.Body().B,buf,0)
		if err!=nil { return }
		blob = buf[:decomp]
		ok = true
		return
	}
	
	blob = append(blobbuf[:0],resp.Body().B...)
	ok = true
	return
}
func (c *Client) Expire(t time.Time) (err error) {
	req  := notrest.AckquireRequest ()
	resp := notrest.AckquireResponse()
	defer notrest.ReleaseRequest (req )
	defer notrest.ReleaseResponse(resp)
	
	req.SetMethodStr("expire")
	{
		path := append(c.tempbuf[:0],"/expire/"...)
		path  = binascii.IntToLe190(binascii.Unsigned(t.Unix()),path)
		req.SetPath(path)
	}
	err = c.Client.Do(req,resp)
	return
}


