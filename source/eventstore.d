module eventstore;

import boilerplate;
// from serialized
import funkwerk.stdx.data.json.parser;
import requests;
import std.algorithm;
import std.datetime;
import std.format;
import std.range;
import std.typecons;
import text.json.Decode;
import text.json.Json;

alias streamUrl = (baseUrl, stream) => format!"%s/streams/%s"(baseUrl, stream);

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

        if (request.isNull)
        {
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