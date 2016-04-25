var viz = {},
    workbook = {},
    LOCATIONS_MAP = {
       'state': 1,
       'district': 2,
       'block': 3,
       'supervisor': 4,
       'awc': 5,
    };

var tableauOptions = {};

function initializeViz(o) {
    tableauOptions = o;

    var placeholderDiv = document.getElementById("tableauPlaceholder");
    var url = tableauOptions.tableauUrl;
    var options = {
        width: placeholderDiv.offsetWidth,
        height: placeholderDiv.offsetHeight,
        hideTabs: true,
        hideToolbar: true,
        onFirstInteractive: function () {
            setUpWorkbook(viz);
            setUpInitialTableauParams();
            setUpNav(viz);
        }
    };
    viz = new tableau.Viz(placeholderDiv, url, options);
}

function setUpWorkbook(viz) {
    workbook = viz.getWorkbook();
}

function setUpInitialTableauParams() {
    var locationKey = 'user_' + tableauOptions.userLocationLevel;
    var params = {
        'view_by': LOCATIONS_MAP[tableauOptions.userLocationLevel],
        locationKey: tableauOptions.userLocation,
    };
    applyParams(workbook, params);

    var historyObject = {
        'sheetName': tableauOptions.currentSheet,
        'params': params
    };
    history.pushState(historyObject, '', tableauOptions.currentSheet)
}

function setUpNav(viz) {
    var sheets = workbook.getPublishedSheetsInfo();
    _.each(sheets, function(sheet) {
        addNavigationLink(sheet.getName(), sheet.getIsActive())
    });

    $(".nav-link").click(function (e){
        var link = $(this);
        var sheetName = link[0].textContent;
        var params = history.state.params;
        switchVisualization(sheetName, workbook, params);
        e.preventDefault();
    });

    viz.addEventListener(tableau.TableauEventName.MARKS_SELECTION, onMarksSelection);
}

function addNavigationLink(sheetName) {
    var html = "<li><a href='#' class='nav-link'>" + sheetName + "</a></li>";
    $(".dropdown-menu").append(html);
}

function onMarksSelection(marksEvent) {
    return marksEvent.getMarksAsync().then(redrawViz);
}

function redrawViz(marks) {
    clearDebugInfo();
    // marks is an array of what the user has clicked
    var html = ["<ul>"],
        // default the sheet to navigate to to the current one
        currentSheet = history.state.sheetName,
        newSheet = currentSheet;
        params = {};

    function buildParams(current_parameters) {
        for (var markIndex = 0; markIndex < marks.length; markIndex++) {
            // TODO: Handle more than 1 better
            var pairs = marks[markIndex].getPairs();
            for (var pairIndex = 0; pairIndex < pairs.length; pairIndex++) {
                var pair = pairs[pairIndex];
                html.push("<li><b>" + pair.fieldName + "</b>: "+ pair.formattedValue + "</li>");

                newSheet = getSheetName(pair, currentSheet);

                // Split out params and filters to be applied
                var PARAM_SUBSTRING = 'ATTR(js_param_',
                        PARAM_UNCHANGED = 'CURRENT';

                if(pair.fieldName.includes(PARAM_SUBSTRING)) {
                    var param_key = pair.fieldName.slice(PARAM_SUBSTRING.length, -1),
                        param_value = pair.formattedValue;

                    if(param_value === PARAM_UNCHANGED) {
                        params[param_key] = lookupParam(current_parameters, param_key);
                    } else {
                        params[param_key] = param_value;
                    }
                }

            }
            html.push("</ul>");
        }
        html = html.join("");
        console.log(html);
        $("#debugbar").append(html);

        // Add inspect button listener
        if( tableauOptions.isDebug ) {
            $("#inspectButton").unbind('click').click(function() {
                switchVisualization(newSheet, workbook, params);
            }).prop('disabled', false);
        } else {
            switchVisualization(newSheet, workbook, params);
        }

    }

    workbook.getParametersAsync()
        .then(buildParams)
        .otherwise(function(err) {
            // TODO: Better error rendering
            alert(err);
        });
}

function getSheetName(pair, sheetName) {
    // Find the sheet we are navigating to
    if((pair.fieldName === 'js_sheet' || pair.fieldName === 'ATTR(js_sheet)') && pair.formattedValue != 'CURRENT') {
        return pair.formattedValue;
    }
    return sheetName;
}

function switchVisualization(sheetName, workbook, params) {
    // TODO: Handle the case where we are in the same sheet, might just need to apply filters then?
    var worksheets;
    workbook.activateSheetAsync(sheetName)
            .then(function(dashboard) {
                worksheets = dashboard.getWorksheets();

                // I need the last worksheet to return a promise like object when all the iteration is complete
                var lastWorksheet = applyParams(workbook, params, lastWorksheet);

                // TODO: historyObject should be an actual object
                var historyObject = {
                        'sheetName': sheetName,
                        'params': params
                    };
                history.pushState(historyObject, sheetName, sheetName);
                return lastWorksheet;
            }).otherwise(function(err){
                // TODO: same thing with this alert as above
                alert(err);
            });

    enableResetFiltersButton();
}

function applyParams(workbook, params, lastWorksheet) {
    _.each(params, function(value, key) {
        // TODO: This is the last param to be set, not the last to finish executing. Need to only run when last param is set (something like _.join())
        lastWorksheet = workbook.changeParameterValueAsync(key, value);
    });
    return lastWorksheet;
}

function clearDebugInfo() {
    $("#debugbar").empty();
    // TODO: Disabling the button doesn't work
    $("#inspectButton").prop('disabled', true);
}

function enableResetFiltersButton() {
    $("#resetFilters").prop('disabled', false).click(function () {
        // TODO: Only bind to this button once
        viz.revertAllAsync();
        disableResetFiltersButton();
    })
}

function disableResetFiltersButton() {
    $("#resetFilters").prop('disabled', true).unbind('click');
}


window.onpopstate = function (event) {
    if(!event.state.sheetName) {
        alert('History object needs a location to navigate to')
    }
    if(event.state.sheetName === 'base') {
        viz.revertAllAsync();
        clearDebugInfo();
    } else {
        workbook.activateSheetAsync(event.state.sheetName).then(function() {
            applyParams(workbook, event.state.params);
        }).otherwise(function(err) {
            alert(err);
        });
    }
}
function lookupParam(params, param_key){
    var matchingParams = _.filter(params, function(param) {
        return param.getName() == param_key;
    })
    // There should be one matching param
    return matchingParams[0].getCurrentValue();
}