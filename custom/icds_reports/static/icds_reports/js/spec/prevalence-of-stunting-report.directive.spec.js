/* global module, inject, _, chai */
"use strict";

var utils = hqImport('icds_reports/js/spec/utils');
var pageData = hqImport('hqwebapp/js/initial_page_data');


describe('Prevalence Of Stunting Directive feature flag disable', function () {

    var $scope, $httpBackend, $location, controller, controllermapOrSectorView;

    pageData.registerUrl('icds-ng-template', 'template');
    pageData.registerUrl('prevalence_of_stunting', 'prevalence_of_stunting');
    pageData.registerUrl('icds_locations', 'icds_locations');

    beforeEach(module('icdsApp', function ($provide) {
        utils.provideDefaultConstants($provide, {includeGenders: true, includeAges: true});
    }));

    beforeEach(inject(function ($rootScope, $compile, _$httpBackend_, _$location_) {
        $scope = $rootScope.$new();
        $httpBackend = _$httpBackend_;
        $location = _$location_;

        $httpBackend.expectGET('template').respond(200, '<div></div>');
        $httpBackend.expectGET('prevalence_of_stunting').respond(200, {
            report_data: ['report_test_data'],
        });
        $httpBackend.expectGET('icds_locations').respond(200, {
            location_type: 'state',
        });
        $scope.test = {};
        var element = window.angular.element("<prevalence-of-stunting></prevalence-of-stunting>");
        var compiled = $compile(element)($scope);
        var mapOrSectorViewElement = window.angular.element("<map-or-sector-view data='test'></map-or-sector-view>");
        var mapOrSectorViewCompiled = $compile(mapOrSectorViewElement)($scope);

        $httpBackend.flush();
        $scope.$digest();
        controller = compiled.controller('prevalenceOfStunting');
        controller.step = 'map';
        controllermapOrSectorView = mapOrSectorViewCompiled.controller('mapOrSectorView');
        controllermapOrSectorView.data = _.clone(utils.controllerMapOrSectorViewData);
    }));

    it('tests instantiate the controller properly', function () {
        chai.expect(controller).not.to.be.a('undefined');
    });

    it('tests initial state', function () {
        assert.equal(controller.mode, 'map');
        assert.equal(controller.steps['map'].label, 'Map View: National');
        assert.deepEqual(controller.filtersData, {});
    });

    it('tests supervisor location', function () {
        controller.filtersData.location_id = 'test-id';
        controller.userLocationId = 'test-id';

        $httpBackend.expectGET('icds_locations?location_id=test-id').respond(200, {location_type: 'supervisor'});
        $httpBackend.expectGET('prevalence_of_stunting?location_id=test-id').respond(200, {
            report_data: ['report_test_data'],
        });
        controller.init();
        $httpBackend.flush();
        assert.equal(controller.mode, 'sector');
        assert.equal(controller.steps['map'].label, 'Sector View');
        assert.deepEqual(controller.data.mapData, ['report_test_data']);
    });

    it('tests non supervisor location', function () {
        controller.filtersData.location_id = 'test-id';
        controller.userLocationId = 'test-id';

        $httpBackend.expectGET('icds_locations?location_id=test-id').respond(200, {location_type: 'non supervisor'});
        $httpBackend.expectGET('prevalence_of_stunting?location_id=test-id').respond(200, {
            report_data: ['report_test_data'],
        });
        controller.init();
        $httpBackend.flush();
        assert.equal(controller.mode, 'map');
        assert.equal(controller.steps['map'].label, 'Map View: Non supervisor');
        assert.deepEqual(controller.data.mapData, ['report_test_data']);
    });

    it('tests template popup', function () {
        var result = controller.templatePopup({properties: {name: 'test'}}, {total: 20, total_measured: 15, severe: 5, moderate: 5, normal: 5});
        assert.equal(result, '<div class="hoverinfo" style="max-width: 200px !important; white-space: normal;">' +
            '<p>test</p>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>20</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>15</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>5</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>33.33%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>33.33%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>33.33%</strong></div></div>');
    });

    it('tests location change', function () {
        controller.init();
        controller.selectedLocations.push(
            {name: 'name1', location_id: 'test_id1'},
            {name: 'name2', location_id: 'test_id2'},
            {name: 'name3', location_id: 'test_id3'},
            {name: 'name4', location_id: 'test_id4'},
            {name: 'name5', location_id: 'test_id5'},
            {name: 'name6', location_id: 'test_id6'}
        );
        $httpBackend.expectGET('prevalence_of_stunting').respond(200, {
            report_data: ['report_test_data'],
        });
        $scope.$digest();
        $httpBackend.flush();
        assert.equal($location.search().location_id, 'test_id4');
        assert.equal($location.search().selectedLocationLevel, 3);
        assert.equal($location.search().location_name, 'name4');
    });

    it('tests moveToLocation national', function () {
        controller.moveToLocation('national', -1);

        var searchData = $location.search();

        assert.equal(searchData.location_id, '');
        assert.equal(searchData.selectedLocationLevel, -1);
        assert.equal(searchData.location_name, '');
    });

    it('tests moveToLocation not national', function () {
        controller.moveToLocation({location_id: 'test-id', name: 'name'}, 3);

        var searchData = $location.search();

        assert.equal(searchData.location_id, 'test-id');
        assert.equal(searchData.selectedLocationLevel, 3);
        assert.equal(searchData.location_name, 'name');
    });

    it('tests chart options', function () {
        var chart = controller.chartOptions.chart;
        var caption = controller.chartOptions.caption;
        assert.notEqual(chart, null);
        assert.notEqual(caption, null);
        assert.equal(controller.chartOptions.chart.type, 'lineChart');
        assert.deepEqual(controller.chartOptions.chart.margin, {
            top: 20,
            right: 60,
            bottom: 60,
            left: 80,
        });
        assert.equal(controller.chartOptions.chart.clipVoronoi, false);
        assert.equal(controller.chartOptions.chart.xAxis.axisLabel, '');
        assert.equal(controller.chartOptions.chart.xAxis.showMaxMin, true);
        assert.equal(controller.chartOptions.chart.xAxis.axisLabelDistance, -100);
        assert.equal(controller.chartOptions.chart.yAxis.axisLabel, '');
        assert.equal(controller.chartOptions.chart.yAxis.axisLabelDistance, 20);
        assert.equal(controller.chartOptions.caption.enable, true);
        assert.deepEqual(controller.chartOptions.caption.css, {
            'text-align': 'center',
            'margin': '0 auto',
            'width': '900px',
        });
        assert.equal(controller.chartOptions.caption.html,
            '<i class="fa fa-info-circle"></i> ' +
            'Of the children enrolled for Anganwadi services, whose height was measured, the percentage of children between (0 - 5 years) who were moderately/severely stunted in the current month. \n' +
            '\n' +
            'Stunting is a sign of chronic undernutrition and has long lasting harmful consequences on the growth of a child'
        );
    });

    it('tests chart tooltip content', function () {
        var month = {value: "Jul 2017", series: []};

        var expected = '<p><strong>Jul 2017</strong></p><br/>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>20</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>10</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>10</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>20.00%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>15.00%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>10.00%</strong></div>';

        var result = controller.tooltipContent(month.value, 0.1, 0.15, 0.2, 10, 20);
        assert.equal(expected, result);
    });

    it('tests horizontal chart tooltip content', function () {
        var expected = '<div class="hoverinfo" style="max-width: 200px !important; white-space: normal;">' +
            '<p>Ambah</p>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>0</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>0</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>0</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>NaN%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>NaN%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>NaN%</strong></div></div>';
        controllermapOrSectorView.templatePopup = function (d) {
            return controller.templatePopup(d.loc, d.row);
        };
        var result = controllermapOrSectorView.chartOptions.chart.tooltip.contentGenerator(utils.d);
        assert.equal(expected, result);
    });

    it('tests disable locations for user', function () {
        controller.userLocationId = 'test_id4';
        controller.location = {name: 'name4', location_id: 'test_id4'};
        controller.selectedLocations.push(
            {name: 'name1', location_id: 'test_id1', user_have_access: 0},
            {name: 'name2', location_id: 'test_id2', user_have_access: 0},
            {name: 'name3', location_id: 'test_id3', user_have_access: 0},
            {name: 'name4', location_id: 'test_id4', user_have_access: 1},
            {name: 'All', location_id: 'all', user_have_access: 0},
            null
        );
        var index = controller.getDisableIndex();
        assert.equal(index, 2);
    });

    it('tests reset additional filters', function () {
        controller.filtersData.gender = 'test';
        controller.filtersData.age = 'test';
        controller.resetAdditionalFilter();

        assert.equal(controller.filtersData.gender, null);
        assert.equal(controller.filtersData.age, null);
    });

    it('tests reset only age additional filters', function () {
        controller.filtersData.gender = 'test';

        controller.resetOnlyAgeAdditionalFilter();
        assert.equal(controller.filtersData.gender, 'test');
        assert.equal(controller.filtersData.age, null);
    });
});


describe('Prevalence Of Stunting Directive  feature flag enable', function () {

    var $scope, $httpBackend, $location, controller, controllermapOrSectorView;

    pageData.registerUrl('icds-ng-template', 'template');
    pageData.registerUrl('prevalence_of_stunting', 'prevalence_of_stunting');
    pageData.registerUrl('icds_locations', 'icds_locations');

    beforeEach(module('icdsApp', function ($provide) {
        utils.provideDefaultConstants($provide, {includeGenders: true, includeAges: true});
        $provide.constant("haveAccessToFeatures", true);
    }));

    beforeEach(inject(function ($rootScope, $compile, _$httpBackend_, _$location_) {
        $scope = $rootScope.$new();
        $httpBackend = _$httpBackend_;
        $location = _$location_;

        $httpBackend.expectGET('template').respond(200, '<div></div>');
        $httpBackend.expectGET('prevalence_of_stunting').respond(200, {
            report_data: ['report_test_data'],
        });
        $httpBackend.expectGET('icds_locations').respond(200, {
            location_type: 'state',
        });
        $scope.test = {};
        var element = window.angular.element("<prevalence-of-stunting></prevalence-of-stunting>");
        var compiled = $compile(element)($scope);
        var mapOrSectorViewElement = window.angular.element("<map-or-sector-view data='test'></map-or-sector-view>");
        var mapOrSectorViewCompiled = $compile(mapOrSectorViewElement)($scope);

        $httpBackend.flush();
        $scope.$digest();
        controller = compiled.controller('prevalenceOfStunting');
        controller.step = 'map';
        controllermapOrSectorView = mapOrSectorViewCompiled.controller('mapOrSectorView');
        controllermapOrSectorView.data = _.clone(utils.controllerMapOrSectorViewData);
    }));

    it('tests instantiate the controller properly', function () {
        chai.expect(controller).not.to.be.a('undefined');
    });

    it('tests initial state', function () {
        assert.equal(controller.mode, 'map');
        assert.equal(controller.steps['map'].label, 'Map View: National');
        assert.deepEqual(controller.filtersData, {});
    });

    it('tests supervisor location', function () {
        controller.filtersData.location_id = 'test-id';
        controller.userLocationId = 'test-id';

        $httpBackend.expectGET('icds_locations?location_id=test-id').respond(200, {location_type: 'supervisor'});
        $httpBackend.expectGET('prevalence_of_stunting?location_id=test-id').respond(200, {
            report_data: ['report_test_data'],
        });
        controller.init();
        $httpBackend.flush();
        assert.equal(controller.mode, 'sector');
        assert.equal(controller.steps['map'].label, 'Sector View');
        assert.deepEqual(controller.data.mapData, ['report_test_data']);
    });

    it('tests non supervisor location', function () {
        controller.filtersData.location_id = 'test-id';
        controller.userLocationId = 'test-id';

        $httpBackend.expectGET('icds_locations?location_id=test-id').respond(200, {location_type: 'non supervisor'});
        $httpBackend.expectGET('prevalence_of_stunting?location_id=test-id').respond(200, {
            report_data: ['report_test_data'],
        });
        controller.init();
        $httpBackend.flush();
        assert.equal(controller.mode, 'map');
        assert.equal(controller.steps['map'].label, 'Map View: Non supervisor');
        assert.deepEqual(controller.data.mapData, ['report_test_data']);
    });

    it('tests template popup', function () {
        var result = controller.templatePopup({properties: {name: 'test'}}, {total: 20, total_measured: 15, severe: 5, moderate: 5, normal: 5});
        assert.equal(result, '<div class="hoverinfo" style="max-width: 200px !important; white-space: normal;">' +
            '<p>test</p>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>20</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>15</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>5</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>33.33%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>33.33%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>33.33%</strong></div></div>');
    });

    it('tests location change', function () {
        controller.init();
        controller.selectedLocations.push(
            {name: 'name1', location_id: 'test_id1'},
            {name: 'name2', location_id: 'test_id2'},
            {name: 'name3', location_id: 'test_id3'},
            {name: 'name4', location_id: 'test_id4'},
            {name: 'name5', location_id: 'test_id5'},
            {name: 'name6', location_id: 'test_id6'}
        );
        $httpBackend.expectGET('prevalence_of_stunting').respond(200, {
            report_data: ['report_test_data'],
        });
        $scope.$digest();
        $httpBackend.flush();
        assert.equal($location.search().location_id, 'test_id4');
        assert.equal($location.search().selectedLocationLevel, 3);
        assert.equal($location.search().location_name, 'name4');
    });

    it('tests moveToLocation national', function () {
        controller.moveToLocation('national', -1);

        var searchData = $location.search();

        assert.equal(searchData.location_id, '');
        assert.equal(searchData.selectedLocationLevel, -1);
        assert.equal(searchData.location_name, '');
    });

    it('tests moveToLocation not national', function () {
        controller.moveToLocation({location_id: 'test-id', name: 'name'}, 3);

        var searchData = $location.search();

        assert.equal(searchData.location_id, 'test-id');
        assert.equal(searchData.selectedLocationLevel, 3);
        assert.equal(searchData.location_name, 'name');
    });

    it('tests chart options', function () {
        var chart = controller.chartOptions.chart;
        var caption = controller.chartOptions.caption;
        assert.notEqual(chart, null);
        assert.notEqual(caption, null);
        assert.equal(controller.chartOptions.chart.type, 'lineChart');
        assert.deepEqual(controller.chartOptions.chart.margin, {
            top: 20,
            right: 60,
            bottom: 60,
            left: 80,
        });
        assert.equal(controller.chartOptions.chart.clipVoronoi, false);
        assert.equal(controller.chartOptions.chart.xAxis.axisLabel, '');
        assert.equal(controller.chartOptions.chart.xAxis.showMaxMin, true);
        assert.equal(controller.chartOptions.chart.xAxis.axisLabelDistance, -100);
        assert.equal(controller.chartOptions.chart.yAxis.axisLabel, '');
        assert.equal(controller.chartOptions.chart.yAxis.axisLabelDistance, 20);
        assert.equal(controller.chartOptions.caption.enable, true);
        assert.deepEqual(controller.chartOptions.caption.css, {
            'text-align': 'center',
            'margin': '0 auto',
            'width': '900px',
        });
        assert.equal(controller.chartOptions.caption.html,
            '<i class="fa fa-info-circle"></i> ' +
            'Of the children enrolled for Anganwadi services, whose height was measured, the percentage of children between (0 - 5 years) who were moderately/severely stunted in the current month. \n' +
            '\n' +
            'Stunting is a sign of chronic undernutrition and has long lasting harmful consequences on the growth of a child'
        );
    });

    it('tests chart tooltip content', function () {
        var month = {value: "Jul 2017", series: []};

        var expected = '<p><strong>Jul 2017</strong></p><br/>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>20</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>10</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>10</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>20.00%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>15.00%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>10.00%</strong></div>';

        var result = controller.tooltipContent(month.value, 0.1, 0.15, 0.2, 10, 20);
        assert.equal(expected, result);
    });

    it('tests horizontal chart tooltip content', function () {
        var expected = '<div class="hoverinfo" style="max-width: 200px !important; white-space: normal;">' +
            '<p>Ambah</p>' +
            '<div>Total Children (0 - 5 years) weighed in given month: <strong>0</strong></div>' +
            '<div>Total Children (0 - 5 years) with height measured in given month: <strong>0</strong></div>' +
            '<div>Number of children (0 - 5 years) unmeasured: <strong>0</strong></div>' +
            '<div>% children (0 - 5 years) with severely stunted growth: <strong>NaN%</strong></div>' +
            '<div>% children (0 - 5 years) with moderate stunted growth: <strong>NaN%</strong></div>' +
            '<div>% children (0 - 5 years) with normal stunted growth: <strong>NaN%</strong></div></div>';
        controllermapOrSectorView.templatePopup = function (d) {
            return controller.templatePopup(d.loc, d.row);
        };
        var result = controllermapOrSectorView.chartOptions.chart.tooltip.contentGenerator(utils.d);
        assert.equal(expected, result);
    });

    it('tests disable locations for user', function () {
        controller.userLocationId = 'test_id4';
        controller.location = {name: 'name4', location_id: 'test_id4'};
        controller.selectedLocations.push(
            {name: 'name1', location_id: 'test_id1', user_have_access: 0},
            {name: 'name2', location_id: 'test_id2', user_have_access: 0},
            {name: 'name3', location_id: 'test_id3', user_have_access: 0},
            {name: 'name4', location_id: 'test_id4', user_have_access: 1},
            {name: 'All', location_id: 'all', user_have_access: 0},
            null
        );
        var index = controller.getDisableIndex();
        assert.equal(index, 2);
    });

    it('tests reset additional filters', function () {
        controller.filtersData.gender = 'test';
        controller.filtersData.age = 'test';
        controller.resetAdditionalFilter();

        assert.equal(controller.filtersData.gender, null);
        assert.equal(controller.filtersData.age, null);
    });

    it('tests reset only age additional filters', function () {
        controller.filtersData.gender = 'test';

        controller.resetOnlyAgeAdditionalFilter();
        assert.equal(controller.filtersData.gender, 'test');
        assert.equal(controller.filtersData.age, null);
    });
});
