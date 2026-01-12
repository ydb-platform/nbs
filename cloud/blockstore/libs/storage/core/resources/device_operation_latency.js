function updateDeviceOperationsData(result, container) {
    if (!result.stat) return;

    container.querySelectorAll('[id*="_"]').forEach(cell => {
        cell.textContent = '';
    });

    const data = {};
    for (const [fullKey, value] of Object.entries(result.stat)) {
        const parts = fullKey.split('_');
        const op = parts[0];
        const agent = parts[1];
        const type = parts[2];
        const bucket = parts[3];
        const key = `${op}_${agent}_${bucket}`;

        if (!data[key])
            data[key] = {};
        data[key][type] = value;
    }

    for (const [key, values] of Object.entries(data)) {
        const cell = container.querySelector('#' + key);
        if (!cell) continue;

        const finished = values.finished || '0';
        const inflight = values.inflight || '0';

        if (finished !== '0' && inflight !== '0') {
            cell.textContent = finished + ' ' + inflight;
        }
        else if (finished !== '0') {
            cell.textContent = finished;
        }
        else if (inflight !== '0') {
            cell.textContent = inflight;
        }
    }
}
