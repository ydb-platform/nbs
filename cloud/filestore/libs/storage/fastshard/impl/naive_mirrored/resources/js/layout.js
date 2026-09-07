function fslCollectStats() {
    var btn = document.getElementById("fsl-stats-btn");
    var loader = document.getElementById("fsl-stats-loader");
    var result = document.getElementById("fsl-stats-result");

    btn.disabled = true;
    loader.hidden = false;
    result.textContent = "";

    var params = new URLSearchParams(window.location.search);
    params.set("action", "fastShardStatsJson");

    fetch(window.location.pathname + "?" + params.toString())
        .then(function (response) {
            if (!response.ok) {
                throw new Error("http status " + response.status);
            }
            return response.json();
        })
        .then(function (data) {
            fslRenderStats(data, result);
        })
        .catch(function (e) {
            fslRenderError(String(e), result);
        })
        .finally(function () {
            btn.disabled = false;
            loader.hidden = true;
        });
}

function fslRenderError(message, result) {
    var div = document.createElement("div");
    div.className = "fsl-error";
    div.textContent = message;
    result.replaceChildren(div);
}

function fslRenderStats(data, result) {
    if (data.error) {
        fslRenderError(data.error, result);
        return;
    }

    var rows = [
        ["Nodes", data.usedNodeCount, data.totalNodeCount],
        ["Names", data.usedNameCount, data.totalNameCount],
        ["Handles", data.usedHandleCount, data.totalHandleCount],
        ["Pages", data.usedPageCount, data.totalPageCount],
    ];

    var table = document.createElement("table");
    table.className = "fsl-table";

    var thead = table.createTHead();
    var headRow = thead.insertRow();
    ["Structure", "Used", "Total"].forEach(function (text) {
        var th = document.createElement("th");
        th.textContent = text;
        headRow.appendChild(th);
    });

    var tbody = table.createTBody();
    rows.forEach(function (row) {
        var tr = tbody.insertRow();
        row.forEach(function (value) {
            tr.insertCell().textContent = String(value);
        });
    });

    result.replaceChildren(table);
}
