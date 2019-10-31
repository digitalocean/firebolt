## Application Code

Your application only needs to do a few things to start running firebolt:

1. Provide a [config](config.md) file
1. [Register](registry.md) any custom source or node types used in the config
1. Start the firebolt executor 
 
### Start the Firebolt Executor
The Executor materializes the pipeline of nodes you define in your config file and manages the flow of events through them. 
The example below shows the minimum requirement:  creating a new executor and calling `Execute()`.   Firebolt supports clean
shutdown on SIGHUP or SIGINT, handy when running in a container.  Your application can also initiate shutdown by calling 
`Shutdown()`.  

```go
    // first register any firebolt source or node types that are not built-in
    node.GetRegistry().RegisterNodeType("jsonconverter", func() node.Node {
            return &jsonconverter.JsonConverter{}
        }, reflect.TypeOf(([]byte)(nil)), reflect.TypeOf(""))
    
    // start the executor running - it will build the source and nodes that process the stream
    ex, err := executor.New(configFile)
    if err != nil {
        fmt.Printf("failed to initialize firebolt for config file %s: %v\n", configFile, err)
        os.Exit(1)
    }
    ex.Execute() // the call to Execute will block while the app runs
```
