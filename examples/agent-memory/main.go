// Command agent-memory demonstrates the Aerospike store with the layout Go
// Micro agents use: one scoped table per session and a growing conversation
// value rewritten every turn.
//
// Run a fixture first:  make fixture-up
//
//	go run ./examples/agent-memory
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"

	microstore "go-micro.dev/v6/store"

	"github.com/aerospike/go-micro-aerospike"
)

func main() {
	s := aerospike.NewStore(
		microstore.Nodes("127.0.0.1:3100"),
		microstore.Database("test"),
	)
	if err := s.Init(); err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Go Micro agents get a table per session: sha256 hex names, 64 chars.
	sum := sha256.Sum256([]byte("demo-session"))
	sessionTable := hex.EncodeToString(sum[:])

	// Scope confines every call to (namespace, table) without mutating the
	// shared store — the safe way to share one backend between components.
	sess := microstore.Scope(s, "", sessionTable)

	// Agent memory: one JSON value rewritten per turn.
	type memoryState struct {
		Messages []string `json:"messages"`
		Summary  string   `json:"summary"`
	}
	state := memoryState{Messages: []string{"user: hello"}}
	for turn := 0; turn < 5; turn++ {
		state.Messages = append(state.Messages, fmt.Sprintf("agent: reply %d", turn))
		b, _ := json.Marshal(state)
		if err := sess.Write(&microstore.Record{Key: "memory", Value: b}); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("turn %d: wrote %d bytes\n", turn, len(b))
	}

	recs, err := sess.Read("memory")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("read back %d bytes, %d messages\n", len(recs[0].Value), len(state.Messages))

	// Large conversations chunk transparently past the 512 KiB inline threshold.
	big, _ := json.Marshal(memoryState{Messages: []string{string(make([]byte, 600*1024))}})
	if err := sess.Write(&microstore.Record{Key: "memory-big", Value: big}); err != nil {
		log.Fatal(err)
	}
	recs, err = sess.Read("memory-big")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("chunked round trip: %d bytes\n", len(recs[0].Value))

	keys, err := sess.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("keys in session table: %v\n", keys)
}
