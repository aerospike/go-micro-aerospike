// Package register makes the Aerospike store selectable via MICRO_STORE.
// Blank-import it to register the constructor in Go Micro's cmd.DefaultStores:
//
//	import _ "github.com/aerospike/go-micro-aerospike/register"
//
// The core aerospike package has no dependency on Go Micro's cmd package;
// only this subpackage does.
package register

import (
	"go-micro.dev/v6/cmd"

	"github.com/aerospike/go-micro-aerospike"
)

func init() {
	cmd.DefaultStores["aerospike"] = aerospike.NewStore
}
