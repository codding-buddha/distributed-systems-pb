package service

import (
	"github.com/codding-buddha/ds-pb/replication/master"
	"github.com/codding-buddha/ds-pb/replication/node"
	"github.com/codding-buddha/ds-pb/replication/proxy"
	rpcInterfaces "github.com/codding-buddha/ds-pb/replication/rpc"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func testMasterServiceCreation(t *testing.T, addr string, logger utils.Logger) func() {
	master, error := master.StartMaster(addr, logger)
	if error != nil {
		t.Fatalf("Failed to start master because %v", error)
	}

	return func() {
		master.Shutdown()
	}
}

func testNodeCreation(t *testing.T, addr string, mAddr string, logger utils.Logger) func() {
	store, err := storage.New(&logger, addr)
	if err != nil {
		t.Fatalf("Failed to start node %s because %v", addr, err)
	}

	node := node.NewChainReplicationNode(addr, store, mAddr, logger)
	err = node.Start()

	if err != nil {
		t.Fatalf("Failed to start node %s because %v", addr, err)
	}
	return func() {
		node.Shutdown()
	}
}

func cleanUpFiles(files ...string) {
	for _, file := range files {
		os.Remove(file)
	}
}

func TestSingleNode(t *testing.T) {
	logger := utils.NewConsole(true)
	mAddr := "localhost:9000"
	sAddr := "localhost:9001"
	cleanUpFiles(mAddr, sAddr)
	mCleanup := testMasterServiceCreation(t, mAddr, *logger)
	defer mCleanup()
	nCleanup := testNodeCreation(t, sAddr, mAddr, *logger)
	defer nCleanup()
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:9003", mAddr, logger)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		client.Update(updateKey.String(), updateValue.String())
		response = <-replyChannel
		client.Query(updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}
}

func TestTwoNodeSetup(t *testing.T) {
	logger := utils.NewConsole(true)
	mAddr := "localhost:9000"
	s1Addr := "localhost:9001"
	s2Addr := "localhost:9002"
	cleanUpFiles(mAddr, s1Addr, s2Addr)
	mCleanup := testMasterServiceCreation(t, mAddr, *logger)
	defer mCleanup()
	n1Cleanup := testNodeCreation(t, s1Addr, mAddr, *logger)
	n2Cleanup := testNodeCreation(t, s2Addr, mAddr, *logger)
	defer n1Cleanup()
	defer n2Cleanup()
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:9003", mAddr, logger)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		client.Update(updateKey.String(), updateValue.String())
		response = <-replyChannel
		client.Query(updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}
}

func TestNNodeSetupWithTailFailure(t *testing.T) {
	logger := utils.NewConsole(true)
	mAddr := "localhost:9000"
	nodes := []string {
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
		"localhost:9004",
	}

	mCleanup := testMasterServiceCreation(t, mAddr, *logger)
	defer mCleanup()
	var cleanUpNodes []func()
	for _, node := range nodes {
		cleanUpFiles(node)
		cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, node, mAddr, *logger))
	}

	casesCount := 10
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:" + strconv.Itoa(9000 + len(nodes) + 1), mAddr, logger)
	tailKilled := false
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		client.Update(updateKey.String(), updateValue.String())
		if i > 3 && !tailKilled {
			cleanUpNodes[len(cleanUpNodes) - 1]()
			tailKilled = true
		}
		response = <-replyChannel

		client.Query(updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}

	defer cCleanup()
	for index, c := range cleanUpNodes {
		if index < len(cleanUpNodes) - 1 {
			c()
		}
	}
}

func testClientCreation(t *testing.T, addr string, mAddr string, logger *utils.Logger) (*proxy.ServiceClient, <-chan *rpcInterfaces.ResponseCallback, func()) {
	client, error := proxy.InitClient(addr, mAddr, *logger)

	if error != nil {
		t.Fatalf("Failed to initialize client : %v", error)
		return client, nil, func() {
		}
	}

	replyChannel := client.OnReply()
	return client, replyChannel, func() {
		client.Shutdown()
	}
}
