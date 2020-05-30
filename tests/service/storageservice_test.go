package service

import (
	"context"
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

func testMasterServiceCreation(t *testing.T, addr string) func() {
	logger := utils.NewConsole(true, addr)
	master, error := master.StartMaster(addr, *logger)
	if error != nil {
		t.Fatalf("Failed to start master because %v", error)
	}

	return func() {
		master.Shutdown()
	}
}

func testNodeCreation(t *testing.T, addr string, mAddr string) func() {
	logger := utils.NewConsole(true, addr)
	store, err := storage.New(logger, addr)
	if err != nil {
		t.Fatalf("Failed to start node %s because %v", addr, err)
	}

	node := node.NewChainReplicationNode(addr, store, mAddr, *logger)
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
	mAddr := "localhost:9000"
	sAddr := "localhost:9001"
	cleanUpFiles(mAddr, sAddr)
	mCleanup := testMasterServiceCreation(t, mAddr)
	defer mCleanup()
	nCleanup := testNodeCreation(t, sAddr, mAddr)
	defer nCleanup()
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:9003", mAddr)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(context.Background(), updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key, found "+response.Error)
		notification := make(chan *rpcInterfaces.ResponseCallback)
		client.UpdateAsync(context.Background(), updateKey.String(), updateValue.String(), notification)
		<-notification
		client.Query(context.Background(), updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}
}

func TestTwoNodeSetup(t *testing.T) {
	mAddr := "localhost:9000"
	s1Addr := "localhost:9001"
	s2Addr := "localhost:9002"
	cleanUpFiles(mAddr, s1Addr, s2Addr)
	mCleanup := testMasterServiceCreation(t, mAddr)
	defer mCleanup()
	n1Cleanup := testNodeCreation(t, s1Addr, mAddr)
	n2Cleanup := testNodeCreation(t, s2Addr, mAddr)
	defer n1Cleanup()
	defer n2Cleanup()
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:9003", mAddr)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(context.Background(), updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		client.Update(context.Background(), updateKey.String(), updateValue.String())
		response = <-replyChannel
		client.Query(context.Background(), updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}
}

func TestNNodeSetupWithTailFailure(t *testing.T) {
	mAddr := "localhost:9000"
	nodes := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
		"localhost:9004",
	}

	mCleanup := testMasterServiceCreation(t, mAddr)
	defer mCleanup()
	var cleanUpNodes []func()
	for _, node := range nodes {
		cleanUpFiles(node)
		cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, node, mAddr))
	}

	casesCount := 10
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1), mAddr)
	tailKilled := false
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(context.Background(), updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		notification := make(chan *rpcInterfaces.ResponseCallback)
		client.UpdateAsync(context.Background(), updateKey.String(), updateValue.String(), notification)
		if i > 3 && !tailKilled {
			cleanUpNodes[len(cleanUpNodes)-1]()
			tailKilled = true
		}
		<-notification

		client.Query(context.Background(), updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}

	defer cCleanup()
	for index, c := range cleanUpNodes {
		if index < len(cleanUpNodes)-1 {
			c()
		}
	}
}

func TestReconfigurationWithMultipleClients(t *testing.T) {
	clientCount := 5
	mAddr := "localhost:9000"
	nodes := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
		"localhost:9004",
		"localhost:9005",
	}
	nodeFailureIndex := []int { len(nodes) - 3, len(nodes) - 1, len(nodes) - 2}
	for _, failureIndex := range nodeFailureIndex {
		mCleanup := testMasterServiceCreation(t, mAddr)
		var cleanUpNodes []func()
		for _, node := range nodes {
			cleanUpFiles(node)
			cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, node, mAddr))
		}
		var clients []struct {
			proxy        *proxy.ServiceClient
			replyChannel <-chan *rpcInterfaces.ResponseCallback
			cleanup      func()
		}

		for i := 0; i < clientCount; i++ {
			c, r, cl := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1+i), mAddr)
			clients = append(clients, struct {
				proxy        *proxy.ServiceClient
				replyChannel <-chan *rpcInterfaces.ResponseCallback
				cleanup      func()
			}{proxy: c, replyChannel: r, cleanup: cl})
		}

		casesCount := 10
		tailKilled := false
		for i := 0; i < casesCount; i++ {
			lookup := make(map[string]string)
			var notifications []chan *rpcInterfaces.ResponseCallback
			for _, client := range clients {
				notification := make(chan *rpcInterfaces.ResponseCallback)
				updateKey, _ := guuid.NewUUID()
				updateValue, _ := guuid.NewUUID()
				k := updateKey.String() + "-req-" + strconv.Itoa(i+1)
				lookup[k] = updateValue.String()
				notifications = append(notifications, notification)
				client.proxy.UpdateAsync(context.Background(), k, updateValue.String(), notification)
			}

			if i > 3 && !tailKilled {
				cleanUpNodes[failureIndex]()
				tailKilled = true
			}

			// wait for update notifications
			for _, n := range notifications {
				<- n
			}

			client := clients[0]
			for key, value := range lookup {
				client.proxy.Query(context.Background(), key)
				response := <-client.replyChannel
				assert.Empty(t, response.Error, "No error expected")
				assert.Equal(t, value, response.Value)
			}
		}

		for index, c := range cleanUpNodes {
			if index != failureIndex {
				c()
			}
		}

		for _, client := range clients {
			client.cleanup()
		}

		mCleanup()
	}
}

func TestChainExtension(t *testing.T) {
	mAddr := "localhost:9000"
	nodes := []string{
		"localhost:9001",
		"localhost:9002",
		"localhost:9003",
		"localhost:9004",
	}

	mCleanup := testMasterServiceCreation(t, mAddr)
	defer mCleanup()
	var cleanUpNodes []func()
	for _, node := range nodes {
		cleanUpFiles(node)
		cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, node, mAddr))
	}

	casesCount := 10
	client, replyChannel, cCleanup := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+2), mAddr)
	chainExtended := false
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		client.Query(context.Background(), updateKey.String())
		response := <-replyChannel
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		notification := make(chan *rpcInterfaces.ResponseCallback)
		client.UpdateAsync(context.Background(), updateKey.String(), updateValue.String(), notification)
		if !chainExtended && i > 5 {
			chainExtended = true
			cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1), mAddr))
		}

		<-notification
		client.Query(context.Background(), updateKey.String())
		response = <-replyChannel
		assert.Empty(t, response.Error, "No error expected")
		assert.Equal(t, updateValue.String(), response.Value)
	}

	defer cCleanup()
	for index, c := range cleanUpNodes {
		if index < len(cleanUpNodes)-1 {
			c()
		}
	}
}

func testClientCreation(t *testing.T, addr string, mAddr string) (*proxy.ServiceClient, <-chan *rpcInterfaces.ResponseCallback, func()) {
	logger := utils.NewConsole(true, addr)
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
