package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	ma "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-multiaddr"
	manet "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-multiaddr/net"

	cmds "github.com/jbenet/go-ipfs/commands"
)

const ApiPath = "/api/v0"

func Send(req cmds.Request) (cmds.Response, error) {
	addr, err := ma.NewMultiaddr(req.Context().Config.Addresses.API)
	if err != nil {
		return nil, err
	}

	_, host, err := manet.DialArgs(addr)
	if err != nil {
		return nil, err
	}

	url := "http://" + host + ApiPath
	url += "/" + strings.Join(req.Path(), "/")

	// TODO: support other encodings once we have multicodec to decode response
	//       (we shouldn't have to set this here)
	encoding := cmds.JSON
	req.SetOption(cmds.EncShort, encoding)

	query := "?"
	options := req.Options()
	for k, v := range options {
		query += "&" + k + "=" + v.(string)
	}

	httpRes, err := http.Post(url+query, "application/octet-stream", req.Stream())
	if err != nil {
		return nil, err
	}

	res := cmds.NewResponse(req)

	contentType := httpRes.Header["Content-Type"][0]
	contentType = strings.Split(contentType, ";")[0]

	if contentType == "application/octet-stream" {
		res.SetValue(httpRes.Body)
		return res, nil
	}

	// TODO: decode based on `encoding`, using multicodec
	dec := json.NewDecoder(httpRes.Body)

	if httpRes.StatusCode >= http.StatusBadRequest {
		e := cmds.Error{}
		err = dec.Decode(&e)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		res.SetError(e, e.Code)

	} else {
		var v interface{}
		err = dec.Decode(&v)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		res.SetValue(v)
	}

	return res, nil
}
