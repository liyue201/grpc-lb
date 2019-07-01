package zk

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc/grpclog"
	"strings"
	"time"
)

var RegistryDir = "/grpclb"

type Option struct {
	ZkServers      []string
	RegistryDir    string
	ServiceName    string
	ServiceVersion string
	NodeID         string
	NData          NodeData
	SessionTimeout time.Duration
}

type NodeData struct {
	Addr     string
	Metadata map[string]string
}

type Registrar struct {
	opt      Option
	path     string
	nodeInfo string
	conn     *zk.Conn
	cancel   context.CancelFunc
}

func NewRegistrar(opt Option) (*Registrar, error) {
	reg := &Registrar{
		opt:  opt,
		path: opt.RegistryDir + "/" + opt.ServiceName + "/" + opt.ServiceVersion + "/" + opt.NodeID,
	}
	data, _ := json.Marshal(opt.NData)
	reg.nodeInfo = string(data)

	c, err := connect(opt.ZkServers, opt.SessionTimeout)
	if err != nil {
		return nil, err
	}
	reg.conn = c
	return reg, nil
}

func connect(zkServers []string, sessionTimeout time.Duration) (*zk.Conn, error) {
	c, event, err := zk.Connect(zkServers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	ticker := time.NewTimer(time.Second * 10)
	for {
		select {
		case e := <-event:
			if e.State == zk.StateConnected {
				return c, nil
			}
		case <-ticker.C:
			break
		}
	}
	return nil, errors.New("connect zk timeout")
}

// create node one by one
// zk not support "mkdir -p"
func (r *Registrar) register(path string, nodeInfo string) error {
	znodes := strings.Split(path, "/")
	var onepath string
	for i, znode := range znodes {
		if len(znode) == 0 {
			continue
		}
		onepath = onepath + "/" + znode
		exists, _, _ := r.conn.Exists(onepath)
		if exists {
			continue
		}
		var err error
		if i != len(znodes)-1 {
			err = createtNode(r.conn, onepath)
		} else {
			err = createTemporaryNode(r.conn, onepath, nodeInfo)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Registrar) Register() error {
	err := r.register(r.path, r.nodeInfo)
	if err == nil {
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		r.keepalive(ctx)
	}
	return err
}

func (r *Registrar) keepalive(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.conn.State() != zk.StateHasSession {
				err := r.register(r.path, r.nodeInfo)
				if err != nil {
					grpclog.Errorf("Registrar register error, %v\n", err.Error())
				}
			}
		}
	}
}

func (r *Registrar) Unregister() {
	if r.cancel != nil {
		r.cancel()
	}
	r.conn.Close()
	r.conn = nil
}

// create temporary node
func createTemporaryNode(conn *zk.Conn, path string, nodeInfo string) error {
	_, err := conn.Create(path, []byte(nodeInfo), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

// create node
func createtNode(conn *zk.Conn, path string) error {
	_, err := conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}
