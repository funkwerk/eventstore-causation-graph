module app;

import boilerplate;
import funkwerk.stdx.data.json.parser;
import meta.never : never;
import progress;
import requests;
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
import text.json.Decode;
import text.json.Json;

alias streamUrl = (baseUrl, stream) => format!"%s/streams/%s"(baseUrl, stream);
alias write = std.file.write;

auto getStream(T, alias decode = never)(string baseUrl, string stream)
{
    auto range = UrlRange!(T, decode)(baseUrl.streamUrl(stream));

    static assert(isInputRange!(typeof(range)));

    range.popFront;
    return range.joiner;
}

struct UrlRange(T, alias decode = never)
{
    private string nextUrl;

    public T[] front;

    public bool empty()
    {
        return front.empty;
    }

    public void popFront()
    {
        if (this.nextUrl.empty)
        {
            this.front = null;
            return;
        }

        static struct Link
        {
            string relation;

            string uri;

            mixin(GenerateAll);
        }

        static struct Data
        {
            T[] entries;

            Link[] links;

            mixin(GenerateAll);
        }

        const string url = this.nextUrl ~ "?embed=body";

        static Nullable!Request request;

        if (request.isNull) {
            request = Nullable!Request(Request());
            request.get.addHeaders(["Accept": "application/vnd.eventstore.atom+json"]);
        }

        // stderr.writefln!"> %s"(url);

        auto stream = parseJSONStream(request.get.get(url).responseBody);
        auto data = text.json.Decode.decodeJson!(Data, decode)(stream, Data.stringof);
        auto next = data.links.find!(a => a.relation == "next");

        if (!next.empty)
        {
            this.nextUrl = next.front.uri.replace("/20", "/1000");
        }
        else
        {
            this.nextUrl = null;
        }
        this.front = data.entries;
    }
}

struct MetaDataDto
{
    @(Json("$correlationId"))
    @(This.Default)
    Nullable!string correlationId;

    @(Json("$causationId"))
    @(This.Default)
    Nullable!string causationId;

    @(This.Default)
    Nullable!SysTime timestamp;

    mixin(GenerateAll);
}

struct MetaData
{
    @(This.Default)
    Nullable!string correlationId;

    @(This.Default)
    Nullable!string causationId;

    @(This.Default)
    Nullable!SysTime timestamp;

    mixin(GenerateAll);
}

MetaData decode(T : MetaData)(const string metaData)
{
    with (text.json.Decode.decode!MetaDataDto(metaData))
    {
        auto builder = MetaData.Builder!();

        builder.correlationId = correlationId.apply!(a => a.idup);
        builder.causationId = causationId.apply!(a => a.idup);
        builder.timestamp = timestamp;

        return builder.value;
    }
}

EventInfo decode(T : EventInfo)(const EventInfoDto event)
{
    const eventId = event.eventId.idup;
    const eventType = event.streamId.until("-").toUTF8 ~ "." ~ event.eventType;
    const timestamp = event.metaData.timestamp;
    const causationId = event.metaData.causationId;

    return EventInfo(eventId, eventType, timestamp, causationId);
}

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

class Diagram
{
    struct Point
    {
        SysTime time;

        Duration duration;
    }

    private string[] types;

    private Appender!(Point[])[string[]] points;

    public this(string[] types)
    {
        this.types = types;
        this.points = null;
    }

    public void add(SysTime time, Duration duration, string[] subcategory = null)
    {
        points.require(subcategory, appender!(Point[])()) ~= Point(time, duration);
    }

    public string config()
    {
        const string[][] categories = [(const(string)[]).init] ~ this.points.keys.filter!"a !is null".array;

        string label(const string[] category)
        {
            if (category is null)
            {
                return this.types.join(" > ");
            }
            return category.join(" > ");
        }

        float radius(const string[] category)
        {
            import std.math : sqrt;

            const length = this.points[category].data.length;

            return min(2.5f, max(0.3f, 50f / sqrt(cast(double) length)));
        }

        return format!configTemplate(
            categories.map!(a =>
                format!"{
                    label: '%s',
                    pointRadius: %s,
                    pointStyle: 'cross',
                    data: [\n  %-(%s,\n%)]
                }"(
                    label(a),
                    radius(a),
                    this.points[a][].map!((Point a) => format!"{t: new Date('%s'), y: %s}"(
                        a.time.toISOExtString, a.duration.total!"msecs")
                    ),
                ),
            ),
        );
    }
}

enum siteTemplate = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Correlation Chain Stats</title>
<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"
    integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
<script src="https://cdn.jsdelivr.net/npm/jquery@3.5.1/dist/jquery.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@2.9.4/dist/Chart.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-colorschemes@0.4.0/dist/chartjs-plugin-colorschemes.min.js"></script>
<script>
{diagramScripts}
</script>
</head>
<body>
{diagramBodies}
</body>
</html>`;

enum diagramScriptTemplate = `
$(document).ready(function() {
    var ctx = document.getElementById('chart{index}').getContext('2d');
    var chart = new Chart(ctx, {config});
});`;

enum diagramBodyTemplate = `
<h3>{desc}</h3>
<div class="container" style="width: 60%; margin-left: 10%;"><canvas id="chart{index}"></canvas></div>`;

enum configTemplate = `{
    type: 'scatter',
    data: {
        datasets: [%-(%s, %)]
    },
    options: {
        scales: {
            xAxes: [{
                type: 'time',
                distribution: 'linear',
                time: {
                    /* right side of diagram is now */
                    max: new Date(),
                    unit: 'hour',
                }
            }],
            y: {
                type: 'linear',
                ticks: {
                    beginAtZero: true
                }
            }
        },
        plugins: {
            colorschemes: {
                scheme: 'tableau.Tableau10'
            }
        }
    }
}`;

int main(string[] args)
{
    if (args.length <= 1)
    {
        stderr.writefln!"Usage: %s <EventStore base URL (such as http://eventstore:2113/)>"(args[0]);
        return 1;
    }
    const baseUrl = args[1];

    static struct Stream
    {
        string streamId;

        string eventType;

        @(This.Default)
        Nullable!MetaData metaData;

        mixin(GenerateAll);
    }

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

    auto sections = diagrams.keys.sort.array;

    auto diagramScripts = diagrams.byKeyValue.enumerate.map!(
        pair => diagramScriptTemplate
            .replace("{desc}", pair[1].key.join(" > "))
            .replace("{config}", pair[1].value.config)
            .replace("{index}", pair[0].to!string)
    );
    auto diagramBodies = sections.enumerate.map!(
        pair => diagramBodyTemplate
            .replace("{desc}", pair[1].join(" > "))
            .replace("{index}", pair[0].to!string)
    );

    siteTemplate
        .replace("{diagramScripts}", diagramScripts.join)
        .replace("{diagramBodies}", diagramBodies.join)
        .writeTo("eventstore-causation-graph.html");

    stderr.writefln!"Done.";
    return 0;
}

alias writeTo = (range, filename) => filename.write(range);

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
