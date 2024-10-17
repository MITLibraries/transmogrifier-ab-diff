function loadJSONData(elementId, jsonData){
    let container = document.getElementById(elementId);
    const options = { mode: 'view' };
    let data = JSON.parse(jsonData);
    return new JSONEditor(container, options, data);
}