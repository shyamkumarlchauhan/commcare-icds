/* global d3, _ */
var url = hqImport('hqwebapp/js/initial_page_data').reverse;

function NewbornWithLowBirthController($scope, $routeParams, $location, $filter, maternalChildService,
    locationsService, userLocationId, storageService, genders, baseControllersService) {
    baseControllersService.BaseController.call(this, $scope, $routeParams, $location, locationsService,
        userLocationId, storageService);
    var vm = this;
    var genderIndex = _.findIndex(genders, function (x) {
        return x.id === vm.filtersData.gender;
    });
    if (genderIndex !== -1) {
        vm.genderLabel = genders[genderIndex].name;
    }

    vm.label = "Newborns with Low Birth Weight";
    vm.steps = {
        'map': {route: '/low_birth/map', label: 'Map View'},
        'chart': {route: '/low_birth/chart', label: 'Chart View'},
    };
    vm.data = {
        legendTitle: '% Newborns',
    };
    vm.filters = ['age'];

    vm.rightLegend = {
        info: 'Percentage of newborns with born with birth weight less than 2500 grams.',
    };

    vm.templatePopup = function(loc, row) {
        var gender = genderIndex > 0 ? genders[genderIndex].name : '';
        var chosenFilters = gender ? ' (' + gender + ') ' : '';
        var total = row ? $filter('indiaNumbers')(row.all) : 'N/A';
        var lowBirth = row ? $filter('indiaNumbers')(row.low_birth) : 'N/A';
        var percent = row ? d3.format('.2%')(row.low_birth / (row.in_month || 1)) : 'N/A';
        var unweighedPercent = row ? d3.format('.2%')(row.in_month / (row.all || 1)) : 'N/A';
        return vm.createTemplatePopup(
            loc.properties.name,
            [{
                indicator_name: chosenFilters + 'Total Number of Newborns born in given month: ',
                indicator_value: total,
            },
            {
                indicator_name: chosenFilters + 'Number of Newborns with LBW in given month: ',
                indicator_value: lowBirth,
            },
            {
                indicator_name: '% newborns with LBW in given month' + chosenFilters + ': ',
                indicator_value: percent,
            },
            {
                indicator_name: '% Unweighted' + chosenFilters + ': ',
                indicator_value: unweighedPercent,
            }]
        );
    };

    vm.loadData = function () {
        vm.setStepsMapLabel();
        var usePercentage = true;
        var forceYAxisFromZero = false;
        vm.myPromise = maternalChildService.getNewbornLowBirthData(vm.step, vm.filtersData).then(
            vm.loadDataFromResponse(usePercentage, forceYAxisFromZero)
        );
    };

    vm.init();

    var options = {
        'xAxisTickFormat': '%b %Y',
        'yAxisTickFormat': ".2%",
        'captionContent': ' Percentage of newborns with born with birth weight less than 2500 grams. \n' +
        '\n' +
        'Newborns with Low Birth Weight are closely associated with foetal and neonatal mortality and morbidity, inhibited growth and cognitive development, and chronic diseases later in life',
    };
    vm.chartOptions = vm.getChartOptions(options);
    vm.chartOptions.chart.width = 1100;
    vm.chartOptions.chart.color = d3.scale.category10().range();

    vm.tooltipContent = function (monthName, dataInMonth) {
        return "<p><strong>" + monthName + "</strong></p><br/>"
            + "<div>Total Number of Newborns born in given month: <strong>" + $filter('indiaNumbers')(dataInMonth.all) + "</strong></div>"
            + "<div>Number of Newborns with LBW in given month: <strong>" + $filter('indiaNumbers')(dataInMonth.low_birth) + "</strong></div>"
            + "<div>% newborns with LBW in given month: <strong>" + d3.format('.2%')(dataInMonth.y) + "</strong></div>"
            + "<div>% Unweighted: <strong>" + d3.format('.2%')(dataInMonth.in_month / (dataInMonth.all || 1)) + "</strong></div>";
    };

    vm.resetAdditionalFilter = function() {
        vm.filtersData.gender = '';
        $location.search('gender', null);
    };

    vm.showAllLocations = function () {
        return vm.all_locations.length < 10;
    };
}

NewbornWithLowBirthController.$inject = ['$scope', '$routeParams', '$location', '$filter', 'maternalChildService', 'locationsService', 'userLocationId', 'storageService', 'genders', 'baseControllersService'];

window.angular.module('icdsApp').directive('newbornLowWeight', function() {
    return {
        restrict: 'E',
        templateUrl: url('icds-ng-template', 'map-chart'),
        bindToController: true,
        scope: {
            data: '=',
        },
        controller: NewbornWithLowBirthController,
        controllerAs: '$ctrl',
    };
});
