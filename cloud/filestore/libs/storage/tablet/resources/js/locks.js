function render() {
    function buildEntry(tabletId, parentNodeId, name) {
        var li = document.createElement('li');
        var entry = document.createElement('span');
        entry.className = 'lock-entry';

        var icon = document.createElement('span');
        icon.className = 'lock-icon';
        icon.textContent = '🔒';
        entry.appendChild(icon);

        var nameEl = document.createElement('span');
        nameEl.className = 'lock-name';
        nameEl.textContent = parentNodeId + "/" + name;
        entry.appendChild(nameEl);

        li.appendChild(entry);

        return li;
    }

    function loadLocks(tabletId, ul) {
        var url = window.location.pathname + '?&TabletID=' + tabletId
            + '&action=locks&getContent=1';
        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    var errLi = document.createElement('li');
                    errLi.className = 'locks-error';
                    errLi.textContent = 'Error: ' + data.error;
                    ul.appendChild(errLi);
                    return;
                }
                if (!data.locks || data.locks.length === 0) {
                    var emptyLi = document.createElement('li');
                    emptyLi.textContent = '(empty)';
                    ul.appendChild(emptyLi);
                    return;
                }
                data.locks.forEach(function(e) {
                    ul.appendChild(
                        buildEntry(tabletId, e.parentNodeId, e.name));
                });
            })
            .catch(function(err) {
                var errLi = document.createElement('li');
                errLi.className = 'locks-error';
                errLi.textContent = 'Fetch error: ' + err;
                ul.appendChild(errLi);
            });
    }

    window.locksInit = function(tabletId) {
        var container = document.getElementById('locks-container');
        if (!container) {
            return;
        }
        var ul = document.createElement('ul');
        container.appendChild(ul);

        loadLocks(tabletId, ul);
    };
}

render();
