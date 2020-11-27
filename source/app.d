module app;

import boilerplate;
import diagram;
import eventstore;
import html;
import progress;
import std.algorithm;
import std.conv;
import std.datetime;
import std.file;
import std.format;
import std.parallelism;
import std.range;
import std.stdio;
import std.typecons;
import std.utf;

int main(string[] args)
{
    if (args.length <= 1)
    {
        stderr.writefln!"Usage: %s <EventStore base URL (such as http://eventstore:2113/)>"(args[0]);
        return 1;
    }
    const baseUrl = args[1];

    auto taskPool = new TaskPool(10);
    scope(exit) taskPool.stop;

    stderr.writefln!"Read streams.";

    bool isRelevant(const Stream stream)
    {
        // neither projection nor configuration stream describing another stream
        return stream.eventType != "$>" && stream.eventType != "$metadata";
    }

    auto streams = getStream!(Stream, decode)(baseUrl, "$streams").filter!isRelevant.array;

    stderr.writefln!"Read all events.";

    Appender!(string[]) correlationIds;
    bool[string] correlationIdFound;

    foreach (stream; taskPool.parallel(new Bar().iter(streams.map!"a"), 1))
    {
        foreach (event; getStream!(Stream, decode)(baseUrl, stream.streamId))
        {
            if (event.metaData.isNull)
                continue;
            if (event.metaData.get.correlationId.isNull)
                continue;

            const correlationId = event.metaData.get.correlationId.get;

            synchronized
            {
                if (correlationId !in correlationIdFound)
                {
                    correlationIdFound[correlationId] = true;
                    correlationIds ~= correlationId;
                }
            }
        }
    }

    stderr.writefln!"Gather correlation chains.";

    Diagram[string[]] diagrams;

    foreach (correlationId; taskPool.parallel(new Bar().iter(correlationIds.data.map!"a"), 1))
    {
        auto stream = getStream!(EventInfo, decode)(baseUrl, format!"$bc-%s"(correlationId))
                .filter!(a => !a.timestamp.isNull)
                .array.retro.array;

        // extract correlation chains
        foreach (chain; stream.findCorrelationChains)
        {
            auto types = chain.map!"a.eventType".array;

            auto chainTotalTime = chain.back.timestamp.get - chain.front.timestamp.get;

            if (chainTotalTime > 10.seconds) continue; // outlier

            synchronized
            {
                auto diagram = diagrams.require(types, new Diagram(types));

                diagram.add(chain.front.timestamp.get, chainTotalTime);
                if (chain.length > 2)
                {
                    foreach (pair; chain.slide(2))
                    {
                        diagram.add(
                            pair.front.timestamp.get,
                            pair.back.timestamp.get - pair.front.timestamp.get,
                            [pair.front.eventType, pair.back.eventType]);
                    }
                }
            }
        }
    }

    stderr.writefln!"Write result.";

    std.file.write("eventstore-causation-graph.html", diagrams.generateHtml);

    stderr.writefln!"Done.";
    return 0;
}

EventInfo[][] findCorrelationChains(EventInfo[] stream)
out (result; result.all!(a => a.slide(2).all!(pair => pair.front.eventId == pair.back.causationId.get)))
{
    const eventById = assocArray(stream.map!(a => a.eventId), stream);
    const eventByCausation = stream
        .filter!(a => !a.causationId.isNull)
        .map!(a => tuple(a.causationId.get, a)).assocArray;
    // find all events that did not cause further events
    auto terminals = stream.filter!(a => a.eventId !in eventByCausation);

    EventInfo[] findChain(const string eventId)
    {
        if (auto event = eventId in eventById)
        {
            if (event.causationId.isNull)
            {
                return [*event];
            }
            return findChain(event.causationId.get) ~ *event;
        }
        assert(false);
    }

    // construct causation chains for them
    return terminals.map!(a => findChain(a.eventId)).filter!(a => a.length > 1).array;
}

EventInfo decode(T : EventInfo)(const EventInfoDto event)
{
    const eventId = event.eventId.idup;
    const eventType = event.streamId.until("-").toUTF8 ~ "." ~ event.eventType;
    const timestamp = event.metaData.timestamp;
    const causationId = event.metaData.causationId;

    return EventInfo(eventId, eventType, timestamp, causationId);
}

alias decode = eventstore.decode;

struct EventInfoDto
{
    string eventId;

    string streamId;

    string eventType;

    @(This.Default)
    Nullable!MetaData metaData;

    mixin(GenerateAll);
}

struct EventInfo
{
    @ConstRead
    private string eventId_;

    @ConstRead
    private string eventType_;

    @ConstRead
    private Nullable!SysTime timestamp_;

    @ConstRead
    private Nullable!string causationId_;

    mixin(GenerateAll);
}

struct Stream
{
    string streamId;

    string eventType;

    @(This.Default)
    Nullable!MetaData metaData;

    mixin(GenerateAll);
}
