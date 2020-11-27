module html;

import diagram;
import std.algorithm;
import std.conv;
import std.format;
import std.range;

public string generateHtml(const Diagram[string[]] diagrams)
{
    auto sections = diagrams.keys.sort.array;

    auto diagramScripts = diagrams.byKeyValue.enumerate.map!(
        pair => diagramScriptTemplate
            .replace("{desc}", pair[1].key.join(" > "))
            .replace("{config}", pair[1].value.generateConfig)
            .replace("{index}", pair[0].to!string)
    );
    auto diagramBodies = sections.enumerate.map!(
        pair => diagramBodyTemplate
            .replace("{desc}", pair[1].join(" > "))
            .replace("{index}", pair[0].to!string)
    );

    return siteTemplate
        .replace("{diagramScripts}", diagramScripts.join)
        .replace("{diagramBodies}", diagramBodies.join);
}


private string generateConfig(const Diagram diagram)
{
    const string[][] categories = [(const(string)[]).init] ~ diagram.categories.filter!"a !is null".array;

    string label(const string[] category)
    {
        if (category is null)
        {
            return diagram.types.join(" > ");
        }
        return category.join(" > ");
    }

    float radius(const string[] category)
    {
        import std.math : sqrt;

        const length = diagram.points(category).length;

        return min(2.5f, max(0.3f, 50f / sqrt(cast(double) length)));
    }

    return format!configTemplate(
        categories.map!(a =>
            format!`{
                label: '%s',
                pointRadius: %s,
                pointStyle: 'cross',
                data: [
                    %-(%s,
                    %)
                ]
            }`(
                label(a),
                radius(a),
                diagram.points(a).map!((const Diagram.Point a) => format!`{t: new Date('%s'), y: %s}`(
                    a.time.toISOExtString, a.duration.total!"msecs")
                ),
            ),
        ),
    );
}

private enum siteTemplate = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>EventStore Causation Graph</title>
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

private enum diagramScriptTemplate = `
$(document).ready(function() {
    var ctx = document.getElementById('chart{index}').getContext('2d');
    var chart = new Chart(ctx, {config});
});`;

private enum diagramBodyTemplate = `
<h3>{desc}</h3>
<div class="container" style="width: 60%; margin-left: 10%;"><canvas id="chart{index}"></canvas></div>`;

private enum configTemplate = `{
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
