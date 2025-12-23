function parseKey(key) {
    const parts = key.split('_');
    if (parts.length < 4) {
        return null;
    }
    return {
        op: parts[0],
        groupId: parts[1],
        status: parts[2],
        latencyBucket: parts.slice(3).join('_')
    };
}

function updateGroupLatencyTable(result, container) {
    if (!result || !result.stat) {
        return;
    }

    const finished = { Read: {}, Write: {}, Patch: {} };
    const inflight = { Read: {}, Write: {}, Patch: {} };

    for (const key in result.stat) {
        const info = parseKey(key);
        if (!info) {
            continue;
        }
        if (!(info.op in finished)) {
            continue;
        }

        const target = info.status === "finished" ? finished : (info.status === "inflight" ? inflight : null);
        if (!target) {
            continue;
        }

        if (!target[info.op][info.groupId]) {
            target[info.op][info.groupId] = {};
        }
        target[info.op][info.groupId][info.latencyBucket] = result.stat[key];
    }

    ['Read', 'Write', 'Patch'].forEach(op => {
        const tbody = document.querySelector('#latency-table-' + op + ' tbody');
        if (!tbody) {
            return;
        }

        const groups = new Set([...Object.keys(finished[op] || {}), ...Object.keys(inflight[op] || {})]);

        groups.forEach(groupId => {
            let tr = tbody.querySelector('tr[data-group="' + groupId + '"]');
            if (!tr) {
                tr = document.createElement('tr');
                tr.setAttribute('data-group', groupId);

                const tdGroup = document.createElement('td');
                tdGroup.textContent = groupId;
                tr.appendChild(tdGroup);

                const colsCount = document.querySelectorAll('#latency-table-' + op + ' thead th').length - 1;
                for (let i = 0; i < colsCount; ++i) {
                    const td = document.createElement('td');
                    tr.appendChild(td);
                }
                tbody.appendChild(tr);
            }

            const headers = document.querySelectorAll('#latency-table-' + op + ' thead th');
            for (let i = 1; i < headers.length; i++) {
                const bucketKey = headers[i].getAttribute('data-bucket-key');
                const td = tr.children[i];

                const finVal = (finished[op][groupId] && finished[op][groupId][bucketKey]) ? finished[op][groupId][bucketKey] : "";
                const inflightVal = (inflight[op][groupId] && inflight[op][groupId][bucketKey]) ? inflight[op][groupId][bucketKey] : "0";

                td.textContent = "";

                const spanFinished = document.createElement("span");
                spanFinished.textContent = finVal;
                td.appendChild(spanFinished);

                if (inflightVal && inflightVal !== "0") {
                    td.appendChild(document.createTextNode(" "));
                    const spanInflight = document.createElement("span");
                    spanInflight.textContent = inflightVal;
                    td.appendChild(spanInflight);
                }
            }
        });
    });
}
