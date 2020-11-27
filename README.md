# eventstore-causation-graph
Graph the processing delay of EventStore causal event chains over time.

# Usage

```
dub run -b release -- http://localhost:2113
xdg-open eventstore-causation-graph.html
```

Substitute the URL with wherever your eventstore is running.

# EventStore Configuration

- `$by_correlation_id` and `$streams` projections must be running
- AtomPub access must be enabled.
- Every event must have a `metaData` field `timestamp`,
  containing the time that the event was emitted in ISO 8601 format with milliseconds.
  - Example: `2003-02-01T12:00:00.123Z`
