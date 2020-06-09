package experiments

import (
	"bufio"
	"context"
	"fmt"
	"github.com/codding-buddha/ds-pb/replication/proxy"
	"github.com/codding-buddha/ds-pb/utils"
	guuid "github.com/google/uuid"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type kv struct {
	key string
	value string
}

// Measure throughput for strong consistency with varying update percentage
func UpdateAndQueryThroughput(host string, master string) {
	updatePercentages := [] float32 { 0, 0.05, 0.1, .15, .2, .25, .3, .35, .4, .45, .5}
	clientCount := 25
	requestCount := 500
	var timings []time.Duration

	for _, updatePercentage := range updatePercentages {
		var wg sync.WaitGroup
		var clients[]struct {
			proxy    *proxy.ServiceClient
			closer   func()
			requests []kv
		}
		port := 7000
		for i := 0; i < clientCount; i++ {
			p, c := createClient(host+":"+strconv.Itoa(port), master)
			port++
			var requests []kv
			totalUpdateRequests  := int(updatePercentage * float32(requestCount))
			for c := 0; c < requestCount; c++ {
				pair := kv{
					key:   randStr(),
					value: "",
				}

				if c < totalUpdateRequests {
					pair.value = randStr()
				}

				requests = append(requests, pair)
			}

			shuffle(requests)
			clients = append(clients, struct {
				proxy    *proxy.ServiceClient
				closer   func()
				requests []kv
			}{proxy: p, closer: c, requests: requests})
		}

		start := time.Now()
		for _, client := range clients {
			wg.Add(1)
			go executeRequests(client.proxy, client.closer, client.requests, &wg)
		}
		wg.Wait()
		elapsed := time.Since(start)
		timings = append(timings, elapsed)
	}
	fmt.Printf("writing output to file")
	filename := "/data/throughput-" + fmt.Sprintf("%d", time.Now().UnixNano())
	ensureDir(filename)
	output, _ := os.Create(filename)
	defer output.Close()
	w := bufio.NewWriter(output)
	defer w.Flush()
	for i := 0; i < len(timings); i++ {
		w.WriteString(fmt.Sprintf("%f", updatePercentages[i]) + "," + timings[i].String() + "\n")
	}
}

func ensureDir(fileName string) {
	dirName := filepath.Dir(fileName)
	if _, serr := os.Stat(dirName); serr != nil {
		merr := os.MkdirAll(dirName, os.ModePerm)
		if merr != nil {
			panic(merr)
		}
	}
}

func executeRequests(client *proxy.ServiceClient, cleanup func(), requests []kv, wg *sync.WaitGroup)  {
	defer wg.Done()
	defer cleanup()
	replyChannel := make(chan *proxy.Result)
	ctx := context.Background()
	for _, r := range requests {
		if r.value != "" {
			client.Update(ctx, r.key, r.value, replyChannel)
			<- replyChannel
			fmt.Printf("Waiting for update of key %s\n", r.key)
		} else {
			client.Query(ctx, r.key)
			fmt.Printf("Waiting for query of key %s\n", r.key)
		}


		if r.value != "" {
			fmt.Printf("Completed update of key %s\n", r.key)
		} else {
			fmt.Printf("Completed query of key %s\n", r.key)
		}
	}
}

func randStr() string {
	v, _ := guuid.NewUUID()
	return v.String()
}

func shuffle(pairs []kv) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
}

func createClient(addr string, mAddr string) (*proxy.ServiceClient, func()) {
	logger := utils.NewConsole(false, addr)
	client, error := proxy.New(addr, mAddr, "master", *logger)

	if error != nil {
		return client, func() {
		}
	}

	return client, func() {
		client.Shutdown()
	}
}
