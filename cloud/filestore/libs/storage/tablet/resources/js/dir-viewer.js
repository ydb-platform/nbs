function render() {
    // Palette for shard coloring - up to 10 distinct shard colors
    var SHARD_COLORS = [
        '#e74c3c', '#2980b9', '#27ae60', '#8e44ad',
        '#e67e22', '#16a085', '#c0392b', '#2471a3',
        '#1e8449', '#6c3483'
    ];
    var shardColorMap = {};
    var shardColorIdx = 0;

    function getShardColor(shardId) {
        if (!shardColorMap[shardId]) {
            shardColorMap[shardId] =
                SHARD_COLORS[shardColorIdx % SHARD_COLORS.length];
            shardColorIdx++;
            updateLegend();
        }
        return shardColorMap[shardId];
    }

    function updateLegend() {
        var legend = document.getElementById('dv-legend');
        if (!legend) {
            return;
        }
        legend.innerHTML =
            '<span style="font-weight:bold;margin-right:6px">Shards:</span>';
        for (var sid in shardColorMap) {
            var item = document.createElement('span');
            item.className = 'dv-legend-item';
            var swatch = document.createElement('span');
            swatch.className = 'dv-legend-swatch';
            swatch.style.background = shardColorMap[sid];
            var label = document.createElement('span');
            label.textContent = sid;
            item.appendChild(swatch);
            item.appendChild(label);
            legend.appendChild(item);
        }
    }

    function nodeTypeIcon(type) {
        switch (type) {
            case 2: return '📁';
            case 3: return '🔗';
            case 5: return '↪';
            default: return '📄';
        }
    }

    function formatSize(sz) {
        if (sz === 0) {
            return '';
        }

        if (sz < 1024) {
            return sz + 'B';
        }

        if (sz < 1024 * 1024) {
            return (sz / 1024).toFixed(1) + 'KiB';
        }

        if (sz < 1024 * 1024 * 1024) {
            return (sz / 1024 / 1024).toFixed(1) + 'MiB';
        }

        if (sz < 1024 * 1024 * 1024 * 1024) {
            return (sz / 1024 / 1024 / 1024).toFixed(1) + 'GiB';
        }

        return (sz / 1024 / 1024 / 1024 / 1024).toFixed(1) + 'TiB';
    }

    function buildEntry(tabletId, name, node) {
        var li = document.createElement('li');
        var entry = document.createElement('span');
        entry.className = 'dv-entry';

        var isDir = node.type == 2;

        var toggle = document.createElement('span');
        toggle.className = 'dv-toggle';
        toggle.textContent = isDir ? '▶' : ' ';
        entry.appendChild(toggle);

        var icon = document.createElement('span');
        icon.className = 'dv-icon';
        icon.textContent = nodeTypeIcon(node.type);
        entry.appendChild(icon);

        var nameEl = document.createElement('span');
        nameEl.className = 'dv-name' + (isDir ? ' dv-dir' : '');
        nameEl.textContent = name;
        entry.appendChild(nameEl);

        var meta = document.createElement('span');
        meta.className = 'dv-meta';
        var parts = ['#' + node.id];
        if (node.size) {
            parts.push(formatSize(node.size));
        }
        meta.textContent = '(' + parts.join(', ') + ')';
        entry.appendChild(meta);

        if (node.shardId) {
            var badge = document.createElement('span');
            badge.className = 'dv-shard-badge';
            badge.style.background = getShardColor(node.shardId);
            badge.title = 'shard: ' + node.shardId
                + (node.shardNodeName ? ', node: ' + node.shardNodeName : '');
            badge.textContent = node.shardId.length > 16
                ? node.shardId.slice(-12) : node.shardId;
            entry.appendChild(badge);
        }

        li.appendChild(entry);

        if (isDir) {
            var childUl = document.createElement('ul');
            childUl.style.display = 'none';
            li.appendChild(childUl);

            var expanded = false;
            var loaded = false;

            function expand() {
                expanded = true;
                toggle.textContent = '▼';
                childUl.style.display = '';
                if (!loaded) {
                    loaded = true;
                    loadDir(tabletId, node.id, childUl);
                }
            }

            function collapse() {
                expanded = false;
                toggle.textContent = '▶';
                childUl.style.display = 'none';
            }

            toggle.addEventListener('click', function(e) {
                e.stopPropagation();
                expanded ? collapse() : expand();
            });
            nameEl.addEventListener('click', function(e) {
                e.stopPropagation();
                expanded ? collapse() : expand();
            });
        }

        return li;
    }

    function buildNextCookieEntry(nextCookie) {
        var li = document.createElement('li');
        var entry = document.createElement('span');
        entry.className = 'dv-entry';

        var toggle = document.createElement('span');
        toggle.className = 'dv-toggle';
        toggle.textContent = ' ';
        entry.appendChild(toggle);

        var icon = document.createElement('span');
        icon.className = 'dv-icon';
        icon.textContent = '🍪';
        entry.appendChild(icon);

        var nameEl = document.createElement('span');
        nameEl.className = 'dv-name';
        nameEl.textContent = nextCookie;
        entry.appendChild(nameEl);

        li.appendChild(entry);

        return li;
    }

    function loadDir(tabletId, nodeId, ul) {
        var loadingLi = document.createElement('li');
        loadingLi.className = 'dv-loading';
        loadingLi.textContent = 'loading...';
        ul.appendChild(loadingLi);

        var url = window.location.pathname + '?&TabletID=' + tabletId
            + '&action=dirViewer&nodeId=' + nodeId;
        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                ul.removeChild(loadingLi);
                if (data.error) {
                    var errLi = document.createElement('li');
                    errLi.className = 'dv-error';
                    errLi.textContent = 'Error: ' + data.error;
                    ul.appendChild(errLi);
                    return;
                }
                if (!data.entries || data.entries.length === 0) {
                    var emptyLi = document.createElement('li');
                    emptyLi.className = 'dv-meta';
                    emptyLi.textContent = '(empty)';
                    ul.appendChild(emptyLi);
                    return;
                }
                data.entries.forEach(function(e) {
                    ul.appendChild(buildEntry(tabletId, e.name, e.node));
                });
                if (data.nextCookie) {
                    ul.appendChild(buildNextCookieEntry(data.nextCookie));
                }
            })
            .catch(function(err) {
                ul.removeChild(loadingLi);
                var errLi = document.createElement('li');
                errLi.className = 'dv-error';
                errLi.textContent = 'Fetch error: ' + err;
                ul.appendChild(errLi);
            });
    }

    window.dvInit = function(tabletId, rootNodeId) {
        var container = document.getElementById('dv-root');
        if (!container) {
            return;
        }
        var ul = document.createElement('ul');
        container.appendChild(ul);

        // Root entry
        var rootLi = document.createElement('li');
        var rootEntry = document.createElement('span');
        rootEntry.className = 'dv-entry';

        var toggle = document.createElement('span');
        toggle.className = 'dv-toggle';
        toggle.textContent = '▼';
        rootEntry.appendChild(toggle);

        var icon = document.createElement('span');
        icon.className = 'dv-icon';
        icon.textContent = '🏠';
        rootEntry.appendChild(icon);

        var nameEl = document.createElement('span');
        nameEl.className = 'dv-name dv-dir';
        nameEl.textContent = '/ (root #' + rootNodeId + ')';
        rootEntry.appendChild(nameEl);

        rootLi.appendChild(rootEntry);

        var childUl = document.createElement('ul');
        rootLi.appendChild(childUl);
        ul.appendChild(rootLi);

        var expanded = true;

        function collapse() {
            expanded = false;
            toggle.textContent = '▶';
            childUl.style.display = 'none';
        }
        function expand() {
            expanded = true;
            toggle.textContent = '▼';
            childUl.style.display = '';
        }

        toggle.addEventListener('click', function() {
            expanded ? collapse() : expand();
        });
        nameEl.addEventListener('click', function() {
            expanded ? collapse() : expand();
        });

        loadDir(tabletId, rootNodeId, childUl);
    };
}

render();
