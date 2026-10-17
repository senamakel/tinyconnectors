# Storage-needs consumer proof

This independent workspace models a host that links only
`tinyconnectors-bus`. It constructs the re-exported `StorageNeeds` with a full
`CollectionSpec`, including indexes, TTL and search declarations, and exercises
all resource-prefix fields. The test script inspects the resolved normal/build
dependency graph so workspace feature unification cannot hide a TinyBus or
implementation dependency.
