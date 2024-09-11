function loadJSONData(elementId, jsonData){
    let container = document.getElementById(elementId);
    const options = { mode: 'view' };
    let data = JSON.parse(jsonData);
    let editor = new JSONEditor(container, options, data);
}