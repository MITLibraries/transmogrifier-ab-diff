function loadJSONData(elementId, data){
    let container = document.getElementById(elementId);
    const options = { mode: 'view' };
    return new JSONEditor(container, options, data);
}