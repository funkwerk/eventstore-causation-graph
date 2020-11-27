module diagram;

import boilerplate;
import std.array;
import std.datetime;

class Diagram
{
    public struct Point
    {
        SysTime time;

        Duration duration;
    }

    @ConstRead
    private string[] types_;

    @(This.Init!null)
    private Appender!(Point[])[string[]] points_;

    public void add(SysTime time, Duration duration, string[] subcategory = null)
    {
        points_.require(subcategory, appender!(Point[])()) ~= Point(time, duration);
    }

    public const(string)[][] categories() const
    {
        return this.points_.keys;
    }

    public const(Point)[] points(const string[] category) const
    in (category in this.points_)
    {
        return this.points_[category][];
    }

    mixin(GenerateFieldAccessors);
    mixin(GenerateThis);
}
