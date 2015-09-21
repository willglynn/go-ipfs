package republisher

import (
	"sync"
	"time"

	namesys "github.com/ipfs/go-ipfs/namesys"
	pb "github.com/ipfs/go-ipfs/namesys/pb"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
	path "github.com/ipfs/go-ipfs/path"
	"github.com/ipfs/go-ipfs/routing"

	proto "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/gogo/protobuf/proto"
	goprocess "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/goprocess"
	gpctx "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/goprocess/context"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	logging "github.com/ipfs/go-ipfs/vendor/go-log-v1.0.0"
)

var log = logging.Logger("ipns-repub")

const DefaultRebroadcastInterval = time.Hour * 12

type Republisher struct {
	r        routing.IpfsRouting
	resolver namesys.Resolver
	pub      namesys.Publisher
	ps       peer.Peerstore

	Interval time.Duration

	entrylock sync.Mutex
	entries   map[peer.ID]struct{}
}

func NewRepublisher(r routing.IpfsRouting, ps peer.Peerstore) *Republisher {
	return &Republisher{
		r:        r,
		resolver: namesys.NewRoutingResolver(r),
		pub:      namesys.NewRoutingPublisher(r),
		ps:       ps,
		entries:  make(map[peer.ID]struct{}),
		Interval: DefaultRebroadcastInterval,
	}
}

func (rp *Republisher) AddName(id peer.ID) {
	rp.entrylock.Lock()
	defer rp.entrylock.Unlock()
	rp.entries[id] = struct{}{}
}

func (rp *Republisher) Run(proc goprocess.Process) {
	tick := time.NewTicker(rp.Interval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			err := rp.republishEntries(proc)
			if err != nil {
				log.Error(err)
			}
		case <-proc.Closing():
			return
		}
	}
}

func (rp *Republisher) republishEntries(p goprocess.Process) error {
	ctx, cancel := context.WithCancel(gpctx.OnClosingContext(p))
	defer cancel()

	for id, _ := range rp.entries {
		log.Infof("republishing ipns entry for %s", id)
		priv := rp.ps.PrivKey(id)

		pkb, err := priv.GetPublic().Bytes()
		if err != nil {
			return err
		}

		// Look for it locally only
		_, ipnskey := namesys.IpnsKeysForID(pkb)
		vals, err := rp.r.GetValues(ctx, ipnskey, 0)
		if err != nil {
			// not found means we dont have a previously published entry
			continue
		}

		// extract published data from record
		val := vals[0].Val
		e := new(pb.IpnsEntry)
		err = proto.Unmarshal(val, e)
		if err != nil {
			return err
		}
		p := path.Path(e.Value)

		// republish it
		err = rp.pub.Publish(ctx, priv, p)
		if err != nil {
			return err
		}
	}

	return nil
}
