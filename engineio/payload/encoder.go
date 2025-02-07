package payload

import (
	"bytes"
	"encoding/base64"
	"io"

	"github.com/vchitai/go-socket.io/v4/engineio/frame"
	"github.com/vchitai/go-socket.io/v4/engineio/packet"
)

type writerFeeder interface {
	getWriter() (io.Writer, error)
	putWriter(error) error
}

type encoder struct {
	supportBinary bool
	feeder        writerFeeder

	ft         frame.Type
	pt         packet.Type
	header     bytes.Buffer
	frameCache bytes.Buffer
	b64Writer  io.WriteCloser
	rawWriter  io.Writer
}

func (e *encoder) NOOP() []byte {
	return []byte{'6'}
}

func (e *encoder) NextWriter(ft frame.Type, pt packet.Type) (io.WriteCloser, error) {
	w, err := e.feeder.getWriter()
	if err != nil {
		return nil, err
	}
	e.rawWriter = w

	e.ft = ft
	e.pt = pt
	e.frameCache.Reset()

	if ft == frame.Binary {
		e.b64Writer = base64.NewEncoder(base64.StdEncoding, &e.frameCache)
	} else {
		e.b64Writer = nil
	}
	return e, nil
}

func (e *encoder) Write(p []byte) (int, error) {
	if e.b64Writer != nil {
		return e.b64Writer.Write(p)
	}
	return e.frameCache.Write(p)
}

func (e *encoder) Close() error {
	if e.b64Writer != nil {
		_ = e.b64Writer.Close()
	}

	var writeHeader func() error
	if e.ft == frame.Binary {
		writeHeader = e.writeB64Header
	} else {
		writeHeader = e.writeTextHeader
	}

	e.header.Reset()
	err := writeHeader()
	if err == nil {
		_, err = e.header.WriteTo(e.rawWriter)
	}
	if err == nil {
		_, err = e.frameCache.WriteTo(e.rawWriter)
	}
	if werr := e.feeder.putWriter(err); werr != nil {
		return werr
	}
	return err
}

func (e *encoder) writeTextHeader() error {
	return e.header.WriteByte(e.pt.StringByte())
}

func (e *encoder) writeB64Header() error {
	err := e.header.WriteByte(e.pt.StringByte())
	if err == nil {
		err = e.header.WriteByte('b')
	}
	return err
}
