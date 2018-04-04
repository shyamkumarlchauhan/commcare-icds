/* global d3 */
var url = hqImport('hqwebapp/js/initial_page_data').reverse;

function InstitutionalDeliveriesController($scope, $routeParams, $location, $filter, maternalChildService,
    locationsService, userLocationId, storageService, baseControllersService) {
    baseControllersService.BaseController.call(this, $scope, $routeParams, $location, locationsService,
        userLocationId, storageService);
    var vm = this;
    vm.label = "Institutional deliveries";
    vm.steps = {
        'map': {route: '/institutional_deliveries/map', label: 'Map View'},
        'chart': {route: '/institutional_deliveries/chart', label: 'Chart View'},
    };
    vm.data = {
        legendTitle: 'Percentage Children',
    };
    vm.filters = ['gender', 'age'];
    vm.rightLegend = {
        info: 'Percentage of pregnant women who delivered in a public or private medical facility in the last month. Delivery in medical instituitions is associated with a decrease in maternal mortality rate.',
    };

    vm.templatePopup = function(loc, row) {
        var total = row ? $filter('indiaNumbers')(row.all) : 'N/A';
        var children =row ? $filter('indiaNumbers')(row.children) : 'N/A';
        var percent = row ? d3.format('.2%')(row.children / (row.all || 1)) : 'N/A';
        return vm.createTemplatePopup(
            loc.properties.name,
            [{
                indicator_name: 'Total number of pregnant women who delivered in the last month: ',
                indicator_value: total,
            },
            {
                indicator_name: 'Total number of pregnant women who delivered in a public/private medical facilitiy in the last month: ',
                indicator_value: children,
            },
            {
                indicator_name: '% pregnant women who delivered in a public or private medical facility in the last month: ',
                indicator_value: percent,
            }]
        );
    };

    vm.loadData = function () {
        vm.setStepsMapLabel();
        var usePercentage = true;
        var forceYAxisFromZero = false;
        vm.myPromise = maternalChildService.getInstitutionalDeliveriesData(vm.step, vm.filtersData).then(
            vm.loadDataFromResponse(usePercentage, forceYAxisFromZero)
        );
    };

    vm.init();

    $scope.$on('filtersChange', function() {
        vm.loadData();
    });

    vm.getDisableIndex = function () {
        var i = -1;
        window.angular.forEach(vm.selectedLocations, function (key, value) {
            if (key !== null && key.location_id === vm.userLocationId) {
                i = value;
            }
        });
        return i;
    };

    vm.moveToLocation = function(loc, index) {
        if (loc === 'national') {
            $location.search('location_id', '');
            $location.search('selectedLocationLevel', -1);
            $location.search('location_name', '');
        } else {
            $location.search('location_id', loc.location_id);
            $location.search('selectedLocationLevel', index);
            $location.search('location_name', loc.name);
        }
    };

    vm.chartOptions = {
        chart: {
            type: 'lineChart',
            height: 450,
            margin : {
                top: 20,
                right: 60,
                bottom: 60,
                left: 80,
            },
            x: function(d){ return d.x; },
            y: function(d){ return d.y; },

            color: d3.scale.category10().range(),
            useInteractiveGuideline: true,
            clipVoronoi: false,
            tooltips: true,
            xAxis: {
                axisLabel: '',
                showMaxMin: true,
                tickFormat: function(d) {
                    return d3.time.format('%b %Y')(new Date(d));
                },
                tickValues: function() {
                    return vm.chartTicks;
                },
                axisLabelDistance: -100,
            },

            yAxis: {
                axisLabel: '',
                tickFormat: function(d){
                    return d3.format(".2%")(d);
                },
                axisLabelDistance: 20,
                forceY: [0],
            },
            callback: function(chart) {
                var tooltip = chart.interactiveLayer.tooltip;
                tooltip.contentGenerator(function (d) {
                    var dataInMonth = _.find(vm.chartData[0].values, function(num) { return num['x'] === d.value;});
                    return vm.tooltipContent(d3.time.format('%b %Y')(new Date(d.value)), dataInMonth);
                });
                return chart;
            },
        },
        caption: {
            enable: true,
            html: '<i class="fa fa-info-circle"></i> Percentage of pregnant women who delivered in a public or private medical facility in the last month. \n' +
            '\n' +
            'Delivery in medical instituitions is associated with a decrease in maternal mortality rate',
            css: {
                'text-align': 'center',
                'margin': '0 auto',
                'width': '900px',
            }
        },
    };

    vm.tooltipContent = function (monthName, dataInMonth) {
        return "<p><strong>" + monthName + "</strong></p><br/>"
            + "<div>Total number of pregnant women who delivered in the last month: <strong>" + $filter('indiaNumbers')(dataInMonth.all) + "</strong></div>"
            + "<div>Total number of pregnant women who delivered in a public/private medical facilitiy in the last month: <strong>" + $filter('indiaNumbers')(dataInMonth.in_month) + "</strong></div>"
            + "<div>% pregnant women who delivered in a public or private medical facility in the last month: <strong>" + d3.format('.2%')(dataInMonth.y) + "</strong></div>";
    };

    vm.showAllLocations = function () {
        return vm.all_locations.length < 10;
    };
}

InstitutionalDeliveriesController.$inject = ['$scope', '$routeParams', '$location', '$filter', 'maternalChildService', 'locationsService', 'userLocationId', 'storageService', 'baseControllersService'];

window.angular.module('icdsApp').directive('institutionalDeliveries', function() {
    return {
        restrict: 'E',
        templateUrl: url('icds-ng-template', 'map-chart'),
        bindToController: true,
        scope: {
            data: '=',
        },
        controller: InstitutionalDeliveriesController,
        controllerAs: '$ctrl',
    };
});
