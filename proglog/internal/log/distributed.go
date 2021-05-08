package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
	api "utwoo.com/proglog/api/v1"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

// This code defines our distributed log type and a function to create the log.
// The log package will contain the single-server, non-replicated log we wrote earlier,
// and the distributed, replicated log built with Raft.
func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{config: config}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

// setupLog(dataDir string) creates the log for this server, where this server will store
// the user’s records.
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// ***** Set Up Raft *****
// A Raft instance comprises:
// • A finite-state machine that applies the commands you give Raft;
// • A log store where Raft stores those commands;
// • A stable store where Raft stores the cluster’s configuration—the servers
//	 in the cluster, their addresses, and so on;
// • A snapshot store where Raft stores compact snapshots of its data; and
// • A transport that Raft uses to connect with the server’s peers.

// setupRaft(dataDir string) configures and creates the server’s Raft instance
func (l *DistributedLog) setupRaft(dataDir string) error {
	// ** Create finite-state machine
	fsm := &fsm{log: l.log}

	// ** Create Raft's log store
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// We configure our log’s initial offset to 1, as required by Raft.
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1

	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}
	// ***

	// ** Create Raft's stable store
	// The stable store is a key-value store where Raft stores important metadata,
	// like the server’s current term or the candidate the server voted for. Bolt is
	// an embedded and persisted key-value database for Go we’ve used as our stable store.
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// ** Create snapshot store
	// Raft snapshots to recover and restore data efficiently
	// Rather than streaming all the data from the Raft leader, the new server would restore
	// from the snapshot and then get the latest changes from the leader. This is more efficient and
	// less taxing on the leader. You want to snapshot frequently to minimize the
	// difference between the data in the snapshots and on the leader. The retain
	// variable specifies that we’ll keep one snapshot.
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	// ** Create Raft transport
	// We create our transport that wraps a stream layer—a low-level stream abstraction
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// ** Create Raft instance and bootstrap the cluster
	// The config’s LocalID field is the unique ID for this server, and it’s the only config
	// field we must set; the rest are optional, and in normal operation the default
	// config should be fine.
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	// Generally you’ll bootstrap a server configured with itself as the only voter,
	// wait until it becomes the leader, and then tell the leader to add more servers
	// to the cluster. The subsequently added servers don’t bootstrap.
	if l.config.Raft.Bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// ***** Log API *****
// Write public APIs that append records to and read records from the log and wrap Raft. The Dis-
// tributedLog will have the same API as the Log type to make them interchangeable.

// Append appends the record to the log. Raft to apply a command
// that tells the FSM to append the record to the log. Raft runs the process to replicate the command to a
// majority of the Raft servers and ultimately append the record to a majority of Raft servers.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply(reqType RequestType, req proto.Marshaler) wraps Raft’s API to apply requests and
// return their responses. Even though we have only one request type, the
// append request type, I’ve written things that easily support multiple request
// types to show how you would set up your own services when you have different
// requests. In apply(), we marshal the request type and request into bytes that
// Raft uses as the record’s data it replicates. The l.raft.Apply(buf.Bytes(), timeout) call
// has a lot going on behind the scenes, running the steps described in Log
// Replication to replicate the record and append the record to the leader’s log.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second

	future := l.raft.Apply(buf.Bytes(), timeout)
	// The future.Error() API returns an error when something went wrong with Raft’s replication.
	// The future.Error() API doesn’t return your service’s errors.
	if future.Error() != nil {
		return nil, future.Error()
	}
	// The future.Response() API returns what your FSM’s Apply() method returned
	// and, opposed to Go’s convention of using Go’s multiple return values to sep-
	// arate errors, you must return a single value for Raft. In our apply() method we
	// check whether the value is an error with a type assertion.
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read(offset uint64) reads the record for the offset from the server’s log. When
// you’re okay with relaxed consistency, read operations need not go through
// Raft. When you need strong consistency, where reads must be up-to-date
// with writes, then you must go through Raft, but then reads are less efficient
// and take longer.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// ***** Finite-State Machine *****
// Raft defers the running of your business logic to the FSM

// The FSM must access the data it manages. In our service, that’s a log, and the FSM appends records to the log.
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

// Your FSM must implement three methods:
//• Apply(record *raft.Log)—Raft invokes this method after committing a log entry.
//• Snapshot()—Raft periodically calls this method to snapshot its state. For
//	most services, you’ll be able to build a compacted log. we would only
//	set the latest command to restore the current state. Because we’re repli-
//	cating a log itself, we need the full log to restore it.
//• Restore(io.ReadCloser)—Raft calls this to restore an FSM from a snapshot—for instance.
type RequestType uint8

const AppendRequestType RequestType = 0

// Make request type and define our append request type. When we send a request to
// Raft for it to apply, and when we read the request in the FSM’s Apply() method
// to apply it, these request types identify the request and tell us how to handle it.

// In Apply(), we switch on the request type and call the corresponding method
// containing the logic to run the command.
func (fsm *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return fsm.applyAppend(buf[1:])
	}
	return nil
}

// In applyAppend([]byte), we unmarshal
// the request and then append the record to the local log and return the
// response for Raft to send back to where we called raft.Apply() in DistributedLog.Append().
func (fsm *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := fsm.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns an FSMSnapshot that represents a point-in-time snapshot of
// the FSM’s state. In our case that state is our FSM’s log, so call Reader() to
// return an io.Reader that will read all the log’s data.
// Raft calls Snapshot() according to your configured SnapshotInterval (how often Raft
// checks if it should snapshot—default is two minutes) and SnapshotThreshold
// (how many logs since the last snapshot before making a new snapshot—default is 8192).
func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := fsm.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

// Raft calls Restore() to restore an FSM from a snapshot. For example, if we lost
// a server and scaled up a new one, we’d want to restore its FSM. The FSM
// must discard existing state to make sure its state will match the leader’s
// replicated state.
// In our Restore() implementation, we reset the log and configure its initial offset
// to the first record’s offset we read from the snapshot so the log’s offsets match.
// Then we read the records in the snapshot and append them to our new log.
func (fsm *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			fsm.log.Config.Segment.InitialOffset = record.Offset
			if err = fsm.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = fsm.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

// These snapshots serve two purposes: they allow Raft to compact its log so it
// doesn’t store logs whose commands Raft has applied already. And they allow
// Raft to bootstrap new servers more efficiently than if the leader had to replicate
// its entire log again and again.
type snapshot struct {
	reader io.Reader
}

// Raft calls Persist() on the FSMSnapshot we created to write its state to some sink
// that, depending on the snapshot store you configured Raft with.
// We’re using the file snapshot store so that when the snapshot completes, we’ll have a file
// containing all the Raft’s log data
func (snapshot *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, snapshot.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Raft calls Release() when it’s finished with the snapshot.
func (snapshot *snapshot) Release() {}

// ***** Raft Log Store *****
// Raft calls your FSM’s Apply() method with *raft.Log’s read from its managed log
// store. Raft replicates a log and then calls your state machine with the log’s
// records. We’re using our own log as Raft’s log store, but we need to wrap our
// log to satisfy the LogStore interface Raft requires.
var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// Raft uses these APIs to get records and information about the log. We support
// the functionality on our log already and just needed to wrap our existing
// methods. What we call offsets, Raft calls indexes.
func (logStore *logStore) FirstIndex() (uint64, error) {
	return logStore.LowestOffset()
}

func (logStore *logStore) LastIndex() (uint64, error) {
	return logStore.HighestOffset()
}

func (logStore *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := logStore.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

// Raft uses these APIs to append records to its log. Again, we just translate the
// call to our log’s API and our record type. These changes require adding some
// fields to our Record type.
func (logStore *logStore) StoreLog(record *raft.Log) error {
	return logStore.StoreLogs([]*raft.Log{record})
}

func (logStore *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := logStore.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange removes the records between the offsets
// it’s to remove records that are old or stored in a snapshot.
func (logStore *logStore) DeleteRange(min, max uint64) error {
	return logStore.Truncate(max)
}

// ***** Create Stream Layer *****
// Raft uses a stream layer in the transport to provide a low-level stream
// abstraction to connect with Raft servers. Our stream layer must satisfy Raft’s
// StreamLayer interface

// Checks satisfies the raft.StreamLayer interface
var _ raft.StreamLayer = (*StreamLayer)(nil)

// We want to enable encrypted communication between servers with
// TLS, so we need to take in the TLS configs used to accept incoming connections
// (the serverTLSConfig) and create outgoing connections (the peerTLSConfig).
type StreamLayer struct {
	listener        net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	listener net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		listener:        listener,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial makes outgoing connections to
// other servers in the Raft cluster. When we connect to a server, we write the
// RaftRPC byte to identify the connection type so we can multiplex Raft on the
// same port as our Log gRPC requests. (We’ll take a look at multiplexing
// shortly.) If we configure the stream layer with a peer TLS config, we make a
// TLS client-side connection.
func (streamLayer *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if streamLayer.peerTLSConfig != nil {
		conn = tls.Client(conn, streamLayer.peerTLSConfig)
	}
	return conn, err
}

// Accept is the mirror of Dial(). We accept the incoming connection and read the
// byte that identifies the connection and then create a server-side TLS connection.
func (streamLayer *StreamLayer) Accept() (net.Conn, error) {
	conn, err := streamLayer.listener.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if streamLayer.serverTLSConfig != nil {
		return tls.Server(conn, streamLayer.serverTLSConfig), nil
	}
	return conn, nil
}

// Close closes the listener.
func (streamLayer *StreamLayer) Close() error {
	return streamLayer.listener.Close()
}

// Addr returns the listener’s address.
func (streamLayer *StreamLayer) Addr() net.Addr {
	return streamLayer.listener.Addr()
}

// ***** Discovery Integration ******
// The next step to implement Raft in our service is to integrate our Serf-driven
// discovery layer with Raft to make the corresponding change in our Raft
// cluster when the Serf membership changes. Each time you add a server to
// the cluster, Serf will publish an event saying a member joined, and our discov-
// ery.Membership will call its handler’s Join(id, addr string) method. When a server
// leaves the cluster, Serf will publish an event saying a member left, and our
// discovery.Membership will call its handler’s Leave(id string) method. Our distributed
// log will act as our Membership’s handler, so we need to implement those Join()
// and Leave() methods to update Raft.

// Join adds the server to the Raft cluster. We add every server as a
// voter, but Raft supports adding servers as non-voters with the AddNonVoter()
// API. You’d find non-voter servers useful if you wanted to replicate state to
// many servers to serve read only eventually consistent state. Each time you
// add more voter servers, you increase the probability that replications and
// elections will take longer because the leader has more servers it needs to
// communicate with to reach a majority.
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddress := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddress {
			if srv.ID == serverID && srv.Address == serverAddress {
				// server has already joined
				return nil
			}
			// remove the existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddress, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes the server from the cluster. Removing the leader will trigger a new election.
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader blocks until the cluster has elected a leader or times out.
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// Close shuts down the Raft instance and closes the local log. And that wraps
// up the method on our DistributedLog.
func (l *DistributedLog) Close() error {
	shutdown := l.raft.Shutdown()
	if err := shutdown.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

// Raft’s configuration comprises the servers in the cluster and includes each
// server’s ID, address, and suffrage—whether the server votes in Raft elections
// (we don’t need the suffrage in our project). Raft can tell us the address of the
// cluster’s leader, too.
// GetServers converts the data from Raft’s raft.Server type
// into our *api.Server type for our API to respond with.
func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}
