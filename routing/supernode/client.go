package supernode

import (
	"bytes"
	"errors"
	"time"

	proto "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/gogo/protobuf/proto"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"

	key "github.com/ipfs/go-ipfs/blocks/key"
	"github.com/ipfs/go-ipfs/p2p/host"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	routing "github.com/ipfs/go-ipfs/routing"
	pb "github.com/ipfs/go-ipfs/routing/dht/pb"
	proxy "github.com/ipfs/go-ipfs/routing/supernode/proxy"
	logging "github.com/ipfs/go-ipfs/vendor/go-log-v1.0.0"
)

var log = logging.Logger("supernode")

type Client struct {
	peerhost  host.Host
	peerstore peer.Peerstore
	proxy     proxy.Proxy
	local     peer.ID
}

// TODO take in datastore/cache
func NewClient(px proxy.Proxy, h host.Host, ps peer.Peerstore, local peer.ID) (*Client, error) {
	return &Client{
		proxy:     px,
		local:     local,
		peerstore: ps,
		peerhost:  h,
	}, nil
}

func (c *Client) FindProvidersAsync(ctx context.Context, k key.Key, max int) <-chan peer.PeerInfo {
	ctx = logging.ContextWithLoggable(ctx, logging.Uuid("findProviders"))
	defer log.EventBegin(ctx, "findProviders", &k).Done()
	ch := make(chan peer.PeerInfo)
	go func() {
		defer close(ch)
		request := pb.NewMessage(pb.Message_GET_PROVIDERS, string(k), 0)
		response, err := c.proxy.SendRequest(ctx, request)
		if err != nil {
			log.Debug(err)
			return
		}
		for _, p := range pb.PBPeersToPeerInfos(response.GetProviderPeers()) {
			select {
			case <-ctx.Done():
				log.Debug(ctx.Err())
				return
			case ch <- p:
			}
		}
	}()
	return ch
}

func (c *Client) PutValue(ctx context.Context, k key.Key, v []byte) error {
	defer log.EventBegin(ctx, "putValue", &k).Done()
	r, err := makeRecord(c.peerstore, c.local, k, v)
	if err != nil {
		return err
	}
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, string(k), 0)
	pmes.Record = r
	return c.proxy.SendMessage(ctx, pmes) // wrap to hide the remote
}

func (c *Client) GetValue(ctx context.Context, k key.Key) ([]byte, error) {
	defer log.EventBegin(ctx, "getValue", &k).Done()
	msg := pb.NewMessage(pb.Message_GET_VALUE, string(k), 0)
	response, err := c.proxy.SendRequest(ctx, msg) // TODO wrap to hide the remote
	if err != nil {
		return nil, err
	}
	return response.Record.GetValue(), nil
}

func (c *Client) GetValues(ctx context.Context, k key.Key, _ int) ([]routing.RecvdVal, error) {
	defer log.EventBegin(ctx, "getValue", &k).Done()
	msg := pb.NewMessage(pb.Message_GET_VALUE, string(k), 0)
	response, err := c.proxy.SendRequest(ctx, msg) // TODO wrap to hide the remote
	if err != nil {
		return nil, err
	}

	return []routing.RecvdVal{
		{
			Val:  response.Record.GetValue(),
			From: c.local,
		},
	}, nil
}

func (c *Client) Provide(ctx context.Context, k key.Key) error {
	defer log.EventBegin(ctx, "provide", &k).Done()
	msg := pb.NewMessage(pb.Message_ADD_PROVIDER, string(k), 0)
	// FIXME how is connectedness defined for the local node
	pri := []pb.PeerRoutingInfo{
		{
			PeerInfo: peer.PeerInfo{
				ID:    c.local,
				Addrs: c.peerhost.Addrs(),
			},
		},
	}
	msg.ProviderPeers = pb.PeerRoutingInfosToPBPeers(pri)
	return c.proxy.SendMessage(ctx, msg) // TODO wrap to hide remote
}

func (c *Client) FindPeer(ctx context.Context, id peer.ID) (peer.PeerInfo, error) {
	defer log.EventBegin(ctx, "findPeer", id).Done()
	request := pb.NewMessage(pb.Message_FIND_NODE, string(id), 0)
	response, err := c.proxy.SendRequest(ctx, request) // hide remote
	if err != nil {
		return peer.PeerInfo{}, err
	}
	for _, p := range pb.PBPeersToPeerInfos(response.GetCloserPeers()) {
		if p.ID == id {
			return p, nil
		}
	}
	return peer.PeerInfo{}, errors.New("could not find peer")
}

// creates and signs a record for the given key/value pair
func makeRecord(ps peer.Peerstore, p peer.ID, k key.Key, v []byte) (*pb.Record, error) {
	blob := bytes.Join([][]byte{[]byte(k), v, []byte(p)}, []byte{})
	sig, err := ps.PrivKey(p).Sign(blob)
	if err != nil {
		return nil, err
	}
	return &pb.Record{
		Key:       proto.String(string(k)),
		Value:     v,
		Author:    proto.String(string(p)),
		Signature: sig,
	}, nil
}

func (c *Client) Ping(ctx context.Context, id peer.ID) (time.Duration, error) {
	defer log.EventBegin(ctx, "ping", id).Done()
	return time.Nanosecond, errors.New("supernode routing does not support the ping method")
}

func (c *Client) Bootstrap(ctx context.Context) error {
	return c.proxy.Bootstrap(ctx)
}

var _ routing.IpfsRouting = &Client{}
