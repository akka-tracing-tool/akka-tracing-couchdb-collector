# Akka Tracing CouchDB Collector
A collector and data source (for visualization) for CouchDB database for Akka Tracing Library.

## Configuring collector

This should be put as `collector` key in your `akka_tracing.conf`:

```hocon
{
  className = "couchdb"
  database {
    host = "localhost" // Your host that CouchDB is running on
    port = 5984 // Your port that CouchDB is listening on
    user = test // User (for authorization) (optional)
    password = test // Password (for authorization) (optional)
    useHttps = true // If the client should use HTTPS protocol (recommended)
    compactOnDelete = false // If after running cleanDatabase task client should compact dbs
    replication {} // You can put replication config here as well
  }
  http {
    connectionTimeout = 2000 // HTTP connection timeout in ms
    requestTimeout = 10000 // HTTP request timeout in ms
  }
}
```

## Configuring replication

This can be put under `replication` key in the config above:

```hocon
{
  targets = [ // You can also put multiple targets here
    {
      host = "localhost" // Host to replicate database to
      port = 5984 // Port that the target CouchDB is listening on
      user = test // User on the target CouchDB to use for authentication
      password = test // Password on the target CouchDB to use for authentication
      useHttps = true // If the client should use HTTPS protocol (recommended)
      createDb = true // If the replication should create target databases (requires admin privileges on target CouchDB)
    }
  ]
}
```
