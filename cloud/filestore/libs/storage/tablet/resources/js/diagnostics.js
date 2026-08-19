function diagnosticsRenderTable(container, title, columns, rows) {
    var heading = document.createElement('h3');
    heading.textContent = title;
    container.appendChild(heading);

    if (!rows || rows.length === 0) {
        var empty = document.createElement('div');
        empty.className = 'diagnostics-empty';
        empty.textContent = '(no data)';
        container.appendChild(empty);
        return;
    }

    var table = document.createElement('table');
    table.className = 'table table-bordered table-striped diagnostics-table';
    var head = table.createTHead().insertRow();
    columns.forEach(function(column) {
        var th = document.createElement('th');
        th.textContent = column.title;
        head.appendChild(th);
    });

    rows.forEach(function(row) {
        var tr = table.insertRow();
        columns.forEach(function(column) {
            var td = tr.insertCell();
            var value = row[column.key];
            td.textContent = value === undefined || value === null ? '' : value;
        });
    });
    container.appendChild(table);
}

function diagnosticsInit(tabletId) {
    var content = document.getElementById('diagnostics-content');
    var refresh = document.getElementById('diagnostics-refresh');
    var status = document.getElementById('diagnostics-status');

    function load() {
        refresh.disabled = true;
        status.textContent = 'Loading...';
        var url = window.location.pathname + '?TabletID=' + encodeURIComponent(tabletId)
            + '&action=diagnostics&getContent=1&top=5&topNodes=10';

        fetch(url)
            .then(function(response) { return response.json(); })
            .then(function(data) {
                content.textContent = '';
                if (data.error) {
                    var error = document.createElement('div');
                    error.className = 'alert alert-danger';
                    error.textContent = data.error;
                    content.appendChild(error);
                    return;
                }

                diagnosticsRenderTable(content, 'Shard stats (top 5)', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'load', title: 'Load'}, {key: 'suffer', title: 'Suffer'},
                    {key: 'used_blocks', title: 'Used blocks'},
                    {key: 'total_blocks', title: 'Total blocks'},
                    {key: 'nodes', title: 'Nodes'}], data.shards);
                diagnosticsRenderTable(content, 'Node access stats (top 10)', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'node_id', title: 'Node'}, {key: 'requests', title: 'Requests'},
                    {key: 'access_score', title: 'Access score'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.node_access);
                diagnosticsRenderTable(content, 'Node latency stats (top 5)', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'node_id', title: 'Node'},
                    {key: 'request_type', title: 'Request type'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.node_latency);
                diagnosticsRenderTable(content, 'Request latency stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'request_type', title: 'Request type'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.request_latency);
                diagnosticsRenderTable(content, 'Shard latency stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.shard_latency);

                (data.warnings || []).forEach(function(warning) {
                    var item = document.createElement('div');
                    item.className = 'diagnostics-warning';
                    item.textContent = warning;
                    content.appendChild(item);
                });
            })
            .catch(function(error) {
                content.textContent = '';
                var item = document.createElement('div');
                item.className = 'alert alert-danger';
                item.textContent = 'Failed to load diagnostics: ' + error;
                content.appendChild(item);
            })
            .then(function() {
                refresh.disabled = false;
                status.textContent = '';
            });
    }

    refresh.addEventListener('click', load);
    load();
}
