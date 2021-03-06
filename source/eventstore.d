module eventstore;

import boilerplate;
// from serialized
import funkwerk.stdx.data.json.parser;
import requests;
import std.algorithm;
import std.datetime;
import std.exception;
import std.format;
import std.range;
import std.typecons;
import text.json.Decode;
import text.json.Json;

alias streamUrl = (baseUrl, stream) => format!"%s/streams/%s"(baseUrl, stream);

auto getStream(T, alias decode = never)(string baseUrl, string stream, BasicAuthentication authenticator)
{
    auto range = UrlRange!(T, decode)(baseUrl.streamUrl(stream), authenticator);

    static assert(isInputRange!(typeof(range)));

    range.popFront;
    return range.joiner;
}

struct UrlRange(T, alias decode = never)
{
    private string nextUrl;

    private BasicAuthentication authenticator;

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

        if (request.isNull)
        {
            request = Nullable!Request(Request());
            if (this.authenticator) request.authenticator = authenticator;
            request.get.addHeaders(["Accept": "application/vnd.eventstore.atom+json"]);
        }

        version (none)
        {
            import std.stdio : writefln;

            writefln!"> %s"(url);
        }

        auto response = request.get.get(url);

        if (url.canFind("$streams"))
            enforce(response.code == 200,
                format!"Unexpected response code %s at URL %s Forgot to start the $streams projection?"(
                    response.code, url));
        else
            enforce(response.code == 200,
                format!"Unexpected response code %s at URL %s"(response.code, url));

        auto data = response.responseBody.data;
        auto stream = parseJSONStream(data);
        auto decodedData = text.json.Decode.decodeJson!(Data, decode)(stream, Data.stringof);
        auto next = decodedData.links.find!(a => a.relation == "next");

        if (!next.empty)
        {
            this.nextUrl = next.front.uri.stepSize("1000");
        }
        else
        {
            this.nextUrl = null;
        }
        this.front = decodedData.entries;
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

    // how eventstore thinks causationId should be spelled
    @(Json("$causedBy"))
    @(This.Default)
    Nullable!string causedBy;

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
        builder.causationId = causationId.orElse(causedBy).apply!(a => a.idup);
        builder.timestamp = timestamp;

        return builder.value;
    }
}

private alias orElse = (value, fallback) => value.isNull ? fallback : value;

private string stepSize(string uri, string newValue)
{
    const parts = uri.split("/");

    // - look for <streamName>/<offset>/backward/20 pattern
    // - replace with <streamName>/<offset>/backward/<newValue>
    if (parts.length >= 2 && parts[$ - 2] == "backward")
    {
        return parts[0 .. $ - 1].chain(newValue.only).join("/");
    }
    return uri; // we tried.
}
