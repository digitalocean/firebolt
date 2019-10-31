## Leader Election

Leader election is based on [Apache Zookeeper](http://zookeeper.apache.org).  To enable leader election, configure the
zookeeper ensemble to use at the top level in your firebolt configuration file:

```yaml
zookeeper: 127.0.0.1:2181               # a comma-separated list of Zookeeper nodes; optional but leader election is disabled without
zkleaderelectionpath: /leaderelection   # a zookeeper node path to be used for cluster leader election
```

With a zookeeper connection established, firebolt will run a continuous leader election/re-election process.  To access the
election results, your node must first embed the type `fbcontext.ContextAware`.  Doing so provides access to the firebolt
context, which you can now use to check if the current instance is the leader:

```go
    leader := mynode.Ctx.IsLeader()
```
