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


package server

import "github.com/maxymania/blobserver/binascii"
import "github.com/maxymania/blobserver/istorage"
import "github.com/byte-mug/gocom/notrest/route"
import "github.com/byte-mug/gocom/notrest"
import "time"

func splitz(str []byte, sep byte) ([]byte,[]byte) {
	for i,b := range str {
		if b==sep { return str[:i],str[i+1:] }
	}
	return str,nil
}

type Server struct{
	StorMap map[string]istorage.Storage
}

func (s *Server) WireUp(router *route.Router) {
	router.POST("/blobs/*" ,s.postBlob)
	router.GET ("/blobs/*" ,s.getBlob )
	router.Method("EXPIRE","/expire/*",s.expire)
}

func (s *Server) postBlob(req *notrest.Request, resp *notrest.Response, rest []byte) {
	t := time.Unix(binascii.Signed(binascii.IntFromLe190(rest)),0)
	var scap int64
	var sobj istorage.Storage
	var skey string
	for k,v := range s.StorMap {
		ca := v.FreeStorage()
		if ca<scap { continue }
		scap = ca
		sobj = v
		skey = k
	}
	if sobj==nil {
		resp.Status(500)
		return
	}
	id,ok := sobj.StoreBlob(req.Body().B,t)
	if !ok {
		resp.Status(500)
		return
	}
	resp.SetHeader([]byte("node"),binascii.EncodeLe190([]byte(skey),nil))
	resp.SetHeader([]byte("id"),binascii.EncodeLe190(id,nil))
	resp.Status(204)
}
func (s *Server) getBlob(req *notrest.Request, resp *notrest.Response, rest []byte) {
	A,B := splitz(rest,'/')
	K,_ := binascii.DecodeLe190(A,nil)
	storage,ok := s.StorMap[string(K)]
	if !ok {
		resp.Status(500)
		return
	}
	K,_ = binascii.DecodeLe190(B,K[:0])
	lz4l,ok := storage.LoadBlob(K,resp.Body())
	if !ok {
		resp.Status(500)
		return
	}
	resp.SetIntHeader("lz4-size",lz4l)
	resp.Status(200)
}
func (s *Server) expire(req *notrest.Request, resp *notrest.Response, rest []byte) {
	t := time.Unix(binascii.Signed(binascii.IntFromLe190(rest)),0)
	resp.Status(200)
	for _,storage := range s.StorMap {
		go storage.Expire(t)
	}
}



