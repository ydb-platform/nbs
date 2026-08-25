function diagnosticsRenderTable(container, title, columns, rows, controls, onChange) {
    var heading = document.createElement('div');
    heading.className = 'diagnostics-table-heading';

    var titleElement = document.createElement('h3');
    titleElement.textContent = title;
    heading.appendChild(titleElement);

    var controlsElement = document.createElement('div');
    controlsElement.className = 'diagnostics-table-controls';
    (controls || []).forEach(function(control) {
        var label = document.createElement('label');
        label.className = 'diagnostics-control';
        label.textContent = control.label + ': ';

        var select = document.createElement('select');
        select.className = 'form-control input-sm';
        control.options.forEach(function(option) {
            var optionElement = document.createElement('option');
            optionElement.value = option.value;
            optionElement.textContent = option.label;
            optionElement.selected = option.value === control.value;
            select.appendChild(optionElement);
        });
        select.addEventListener('change', function() {
            control.setValue(select.value);
            onChange();
        });
        label.appendChild(select);
        controlsElement.appendChild(label);
    });
    heading.appendChild(controlsElement);
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
    var settings = {
        topLoaded: '10',
        sortBy: 'load',
        topAccessed: '10',
        batchSize: '10',
        slowestNodes: '10',
        slowestRequests: '10',
        slowestShards: '10'
    };
    var topOptions = [5, 10, 20, 50, 100, 500].map(function(value) {
        return {value: String(value), label: String(value)};
    });
    var batchOptions = [1, 5, 10, 20, 50, 100].map(function(value) {
        return {value: String(value), label: String(value)};
    });
    var sortOptions = [
        {value: 'load', label: 'Load'},
    ];

    function control(label, key, options) {
        return {
            label: label,
            value: settings[key],
            options: options,
            setValue: function(value) { settings[key] = value; }
        };
    }

    function setBatchControl() {
        var container = document.getElementById('diagnostics-controls');
        if (!container) {
            return;
        }
        container.textContent = '';
        var label = document.createElement('label');
        label.className = 'diagnostics-control';
        label.textContent = 'Batch size: ';

        var select = document.createElement('select');
        select.className = 'form-control input-sm';
        batchOptions.forEach(function(option) {
            var optionElement = document.createElement('option');
            optionElement.value = option.value;
            optionElement.textContent = option.label;
            optionElement.selected = option.value === settings.batchSize;
            select.appendChild(optionElement);
        });
        select.addEventListener('change', function() {
            settings.batchSize = select.value;
            load();
        });
        label.appendChild(select);
        container.appendChild(label);
    }

    function load() {
        setBatchControl();
        refresh.disabled = true;
        status.textContent = 'Loading...';
        var url = window.location.pathname + '?TabletID=' + encodeURIComponent(tabletId)
            + '&action=diagnostics&getContent=1'
            + '&topLoaded=' + encodeURIComponent(settings.topLoaded)
            + '&sortBy=' + encodeURIComponent(settings.sortBy)
            + '&topAccessed=' + encodeURIComponent(settings.topAccessed)
            + '&batchSize=' + encodeURIComponent(settings.batchSize)
            + '&slowestNodes=' + encodeURIComponent(settings.slowestNodes)
            + '&slowestRequests=' + encodeURIComponent(settings.slowestRequests)
            + '&slowestShards=' + encodeURIComponent(settings.slowestShards);

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

                diagnosticsRenderTable(content, 'Shard stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'load', title: 'Load'}, {key: 'suffer', title: 'Suffer'},
                    {key: 'used_blocks', title: 'Used blocks'},
                    {key: 'total_blocks', title: 'Total blocks'},
                    {key: 'nodes', title: 'Nodes'}], data.shards, [
                        control('Top', 'topLoaded', topOptions),
                        control('Sort by', 'sortBy', sortOptions)
                    ], load);
                diagnosticsRenderTable(content, 'Node access stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'node_id', title: 'Node'}, {key: 'requests', title: 'Requests'},
                    {key: 'access_score', title: 'Access score'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.node_access, [
                        control('Top', 'topAccessed', topOptions)
                    ], load);
                diagnosticsRenderTable(content, 'Node latency stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'node_id', title: 'Node'},
                    {key: 'request_type', title: 'Request type'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.node_latency, [
                        control('Top', 'slowestNodes', topOptions)
                    ], load);
                diagnosticsRenderTable(content, 'Request latency stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'request_type', title: 'Request type'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.request_latency, [
                        control('Top', 'slowestRequests', topOptions)
                    ], load);
                diagnosticsRenderTable(content, 'Shard latency stats', [
                    {key: 'rank', title: '#'}, {key: 'shard_id', title: 'Shard'},
                    {key: 'requests', title: 'Requests'},
                    {key: 'avg_decayed_us', title: 'Avg decayed (us)'},
                    {key: 'total_decayed_us', title: 'Total decayed (us)'},
                    {key: 'total_us', title: 'Total (us)'},
                    {key: 'last_accessed', title: 'Last accessed'}], data.shard_latency, [
                        control('Top', 'slowestShards', topOptions)
                    ], load);

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
