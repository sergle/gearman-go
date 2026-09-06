// The examples live in their own module so the library itself stays
// dependency-free. example/worker needs github.com/mikespook/golib, which
// would otherwise land in the root go.mod (and drag in mgo.v2, yaml.v2 and
// check.v1) for every consumer of client and worker.
//
// The replace points at the parent so the examples always build against the
// working tree, not a published version.
module github.com/sergle/gearman-go/example

go 1.21

require (
	github.com/mikespook/golib v0.0.0-20151119134446-38fe6917d34b
	github.com/sergle/gearman-go v0.0.0
)

require (
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/sergle/gearman-go => ../
