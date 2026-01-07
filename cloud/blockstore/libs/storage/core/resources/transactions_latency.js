function updateTransactionsData(result, container) {
    if (!result.stat) {
        return;
    }
    for (let key in result.stat) {
        const element = container.querySelector('#' + key);
        if (element) {
            element.textContent = result.stat[key];
        }
    }
}
