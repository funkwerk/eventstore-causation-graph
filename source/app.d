module app;

import boilerplate;
import diagram;
import eventstore;
import html;
import progress;
import requests;
import std.algorithm;
import std.conv;
import std.datetime;
import std.exception;
import std.file;
import std.format;
import std.parallelism;
import std.range;
import std.stdio;
import std.typecons;
import std.utf;

int main(string[] args)
{
    const string[] includes = args.getOptList("include");
    const bool verbose = args.getFlag("verbose");
    const string user = args.getOpt("user");
    const string pass = args.getOpt("pass");
    if (args.length <= 1)
    {
        stderr.writefln!"Usage: %s <url> [--include <stream>]"(args[0]);
        stderr.writefln!"";
        stderr.writefln!"<url>: EventStore base URL (such as http://eventstore:2113)";
        stderr.writefln!"--include <stream>: Include this stream even if it's a projection";
        stderr.writefln!"--verbose: Show correlation IDs in tick labels";
        stderr.writefln!"--user: EventStore user name";
        stderr.writefln!"--pass: EventStore password";
        return 1;
    }
    const baseUrl = args[1];
    BasicAuthentication authenticator = null;

    if (!user.empty)
    {
        authenticator = new BasicAuthentication(user, pass);
    }

    auto taskPool = new TaskPool(10);
    scope(exit) taskPool.stop;

    stderr.writefln!"Read streams.";

    bool isRelevant(const Stream stream)
    {
        if (includes.canFind(stream.streamId)) return true;
        // neither projection nor configuration stream describing another stream
        const isProjection = stream.eventType == "$>";
        const isMetaData = stream.eventType == "$metadata";
        if (isProjection || isMetaData)
        {
            stderr.writefln!"> Excluding stream %s: EventType %s"(stream.streamId, stream.eventType);
            return false;
        }
        return true;
    }

    auto streams = getStream!(Stream, decode)(baseUrl, "$streams", authenticator).filter!isRelevant.array;

    stderr.writefln!"Read all events.";

    EventInfo[][string] eventsByCorrelationId;

    auto b = new Bar;

    b.max = streams.length;
    b.start;
    foreach (stream; taskPool.parallel(streams.map!"a", 1))
    {
        scope(exit) synchronized b.next;
        foreach (event; getStream!(EventInfo, decode)(baseUrl, stream.streamId, authenticator))
        {
            if (event.timestamp.isNull || event.correlationId.isNull)
                continue;

            const correlationId = event.correlationId.get;

            synchronized
            {
                if (!eventsByCorrelationId.get(correlationId, null).canFind!(a => a.eventId == event.eventId))
                {
                    eventsByCorrelationId[correlationId] ~= event;
                }
            }
        }
    }
    b.finish;

    stderr.writefln!"Gather correlation chains.";

    Diagram[string[]] diagrams;

    foreach (correlationId, stream; eventsByCorrelationId)
    {
        auto sortedStream = stream.sort!((a, b) => a.timestamp < b.timestamp).array;

        // extract correlation chains
        foreach (chain; sortedStream.findCorrelationChains)
        {
            auto types = chain.map!"a.eventType".array;

            auto chainTotalTime = chain.back.timestamp.get - chain.front.timestamp.get;

            if (chainTotalTime > 10.seconds) continue; // outlier

            auto diagram = diagrams.require(types, new Diagram(types));

            diagram.add(chain.front.timestamp.get, chainTotalTime, correlationId);
            if (chain.length > 2)
            {
                foreach (pair; chain.slide(2))
                {
                    diagram.add(
                        pair.front.timestamp.get,
                        pair.back.timestamp.get - pair.front.timestamp.get,
                        correlationId,
                        [pair.front.eventType, pair.back.eventType]);
                }
            }
        }
    }

    stderr.writefln!"Write result.";

    std.file.write("eventstore-causation-graph.html", diagrams.generateHtml(verbose));

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
        throw new Exception(format!"dangling causation chain: event %s not known"(eventId));
    }

    EventInfo[] findChainEvent(const EventInfo event)
    {
        return findChain(event.eventId);
    }

    // construct causation chains for them
    return terminals
            .map!(a => findChainEvent(a).exceptionToNull)
            .nonNull
            .filter!(a => a.length > 1)
            .array;
}

auto exceptionToNull(T)(lazy T value)
{
    try
    {
        return Nullable!(typeof(value))(value);
    }
    catch (Exception exc)
    {
        stderr.writefln!"WARNING: Entry ignored: %s"(exc.msg);
        return Nullable!(typeof(value))();
    }
}

alias nonNull = range => range.filter!"!a.isNull".map!"a.get";

EventInfo decode(T : EventInfo)(const EventInfoDto event)
{
    const eventId = event.eventId.idup;
    const eventType = event.streamId.until("-").toUTF8 ~ "." ~ event.eventType;
    const timestamp = event.metaData.apply!"a.timestamp";
    const correlationId = event.metaData.apply!"a.correlationId";
    const causationId = event.metaData.apply!"a.causationId";

    return EventInfo(eventId, eventType, timestamp, correlationId, causationId);
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
    private Nullable!string correlationId_;

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

bool getFlag(ref string[] args, string flag)
{
    const string flagWithDashes = "--" ~ flag;
    const bool found = args.canFind(flagWithDashes);

    args = args.filter!(a => a != flagWithDashes).array;
    return found;
}

string getOpt(ref string[] args, string flag)
{
    string[] result = args.getOptList(flag);

    if (result.empty) return null;
    enforce(result.length == 1, format!"'--%s' passed too many times"(flag));
    return result.front;
}

string[] getOptList(ref string[] args, string flag)
{
    string flagTwoArgs = "--" ~ flag;
    string flagOneArg = "--" ~ flag ~ "=";
    string[] result;
    string[] remainingArgs;
    for (int i = 0; i < args.length; i++)
    {
        if (args[i] == flagTwoArgs)
        {
            enforce(i + 1 < args.length, "missing parameter for " ~ flagTwoArgs);
            result ~= args[++i];
            continue;
        }
        if (args[i].startsWith(flagOneArg))
        {
            result ~= args[i].drop(flagOneArg.length).array.toUTF8;
            continue;
        }
        remainingArgs ~= args[i];
    }
    args = remainingArgs;
    return result;
}
