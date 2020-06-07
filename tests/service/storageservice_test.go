package service

import (
	"context"
	"github.com/codding-buddha/ds-pb/replication/dispatcher"
	"github.com/codding-buddha/ds-pb/replication/master"
	"github.com/codding-buddha/ds-pb/replication/node"
	"github.com/codding-buddha/ds-pb/replication/proxy"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func testMasterServiceCreation(t *testing.T, addr string) func() {
	logger := utils.NewConsole(true, addr)
	master, error := master.StartMaster(addr, *logger, false)
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
	client, cCleanup := testClientCreation(t, "localhost:9003", mAddr)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		response := client.Query(context.Background(), updateKey.String())
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key, found "+response.Error)
		notification := make(chan *proxy.Result)
		client.Update(context.Background(), updateKey.String(), updateValue.String(), notification)
		<-notification
		response = client.Query(context.Background(), updateKey.String())
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
	client, cCleanup := testClientCreation(t, "localhost:9003", mAddr)
	defer cCleanup()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		response := client.Query(context.Background(), updateKey.String())
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		notification := make(chan *proxy.Result)
		client.Update(context.Background(), updateKey.String(), updateValue.String(), notification)
		<-notification
		response = client.Query(context.Background(), updateKey.String())
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
	client, cCleanup := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1), mAddr)
	tailKilled := false
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		response := client.Query(context.Background(), updateKey.String())
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		notification := make(chan *proxy.Result)
		client.Update(context.Background(), updateKey.String(), updateValue.String(), notification)
		if i > 3 && !tailKilled {
			cleanUpNodes[len(cleanUpNodes)-1]()
			tailKilled = true
		}
		<-notification

		response = client.Query(context.Background(), updateKey.String())
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
			cleanup      func()
		}

		for i := 0; i < clientCount; i++ {
			c, cl := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1+i), mAddr)
			clients = append(clients, struct {
				proxy        *proxy.ServiceClient
				cleanup      func()
			}{proxy: c, cleanup: cl})
		}

		casesCount := 10
		tailKilled := false
		for i := 0; i < casesCount; i++ {
			lookup := make(map[string]string)
			var notifications []chan *proxy.Result
			for _, client := range clients {
				notification := make(chan *proxy.Result)
				updateKey, _ := guuid.NewUUID()
				updateValue, _ := guuid.NewUUID()
				k := updateKey.String() + "-req-" + strconv.Itoa(i+1)
				lookup[k] = updateValue.String()
				notifications = append(notifications, notification)
				client.proxy.Update(context.Background(), k, updateValue.String(), notification)
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
				response := client.proxy.Query(context.Background(), key)
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
	client, cCleanup := testClientCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+2), mAddr)
	chainExtended := false
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		response := client.Query(context.Background(), updateKey.String())
		assert.NotEmpty(t, response.Error, "Error msg expected for non existent key")
		notification := make(chan *proxy.Result)
		client.Update(context.Background(), updateKey.String(), updateValue.String(), notification)
		if !chainExtended && i > 5 {
			chainExtended = true
			cleanUpNodes = append(cleanUpNodes, testNodeCreation(t, "localhost:"+strconv.Itoa(9000+len(nodes)+1), mAddr))
		}

		<-notification
		response = client.Query(context.Background(), updateKey.String())
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

func TestDispatcherWithMultipleChains(t *testing.T) {
	m1 := "localhost:9000"
	chain1 := []string{ "localhost:9001", "localhost:9002", "localhost:9003" }
	c1 := createChain(t, chain1, m1)
	m2 := "localhost:8000"
	chain2 := []string{ "localhost:8001", "localhost:8002", "localhost:8003" }
	c2 := createChain(t, chain2, m2)
	defer c1()
	defer c2()

	dAddr := "localhost:6000"
	d := dispatcher.New([]string{m1, m2}, utils.NewConsole(false, dAddr), dAddr)
	d.Start()
	defer d.Shutdown()
	// let chains be created & running
	time.Sleep(10 * time.Second)
	logger := utils.NewConsole(true, "localhost:5003")
	client, _ := proxy.New("localhost:5003", dAddr, "dispatcher", *logger)
	defer client.Shutdown()

	casesCount := 10
	for i := 0; i < casesCount; i++ {
		updateKey, _ := guuid.NewUUID()
		updateValue, _ := guuid.NewUUID()
		result := client.Query(context.Background(), updateKey.String())
		assert.NotEmpty(t, result.Error, "Error msg expected for non existent key, found " + result.Error)
		notification := make(chan *proxy.Result)
		client.Update(context.Background(), updateKey.String(), updateValue.String(), notification)
		<-notification
		result = client.Query(context.Background(), updateKey.String())
		assert.Empty(t, result.Error, "No error expected")
		assert.Equal(t, updateValue.String(), result.Value)
	}
}

func testClientCreation(t *testing.T, addr string, mAddr string) (*proxy.ServiceClient,  func()) {
	logger := utils.NewConsole(true, addr)
	client, error := proxy.New(addr, mAddr, "master", *logger)

	if error != nil {
		t.Fatalf("Failed to initialize client : %v", error)
		return client, func() {
		}
	}
	return client, func() {
		client.Shutdown()
	}
}

func createChain(t *testing.T, addr []string, mAddr string) func() {
	var cleanup []func()
	cleanup = append(cleanup, testMasterServiceCreation(t, mAddr))
	for _, c := range addr {
		cleanup = append( cleanup, testNodeCreation(t, c, mAddr))
	}

	return func() {
		for _, c := range cleanup {
			c()
		}
	}
}
