function MainController($scope, $route, $routeParams, $location) {
    $scope.$route = $route;
    $scope.$location = $location;
    $scope.$routeParams = $routeParams;
    $scope.systemUsageCollapsed = true;
    $scope.healthCollapsed = true;
}

MainController.$inject = ['$scope', '$route', '$routeParams', '$location'];

window.angular.module('icdsApp', ['ngRoute', 'ui.select', 'ngSanitize', 'datamaps', 'ui.bootstrap', 'nvd3'])
    .controller('MainController', MainController)
    .config(['$interpolateProvider', '$routeProvider', function($interpolateProvider, $routeProvider) {
        $interpolateProvider.startSymbol('{$');
        $interpolateProvider.endSymbol('$}');

        $routeProvider
            .when("/", {
                redirectTo : '/program_summary/system_usage',
            }).when("/program_summary/:step", {
                template : "<system-usage></system-usage>"
            }).when("/awc_opened", {
                redirectTo : "/awc_opened/map",
            })
            .when("/awc_opened/:step", {
                template : "<awc-opened-yesterday></awc-opened-yesterday>",
            })
            .when("/active_awws", {
                template : "active_awws",
            })
            .when("/submitted_yesterday", {
                template : "submitted_yesterday",
            })
            .when("/submitted", {
                template : "submitted",
            })
            .when("/system_usage_tabular", {
                template : "system_usage_tabular",
            })
            .when("/underweight_children", {
                redirectTo : "/underweight_children/1",
            })
            .when("/underweight_children/:step", {
                template : "<underweight-children-report></underweight-children-report>",
            })
            .when("/breastfeeding", {
                template : "breastfeeding",
            })
            .when("/exclusive_bf", {
                template : "exclusive_bf",
            })
            .when("/comp_feeding", {
                template : "comp_feeding",
            })
            .when("/health_tabular_report", {
                template : "health_tabular_report",
            })
            .when("/awc_reports", {
                redirectTo : "/awc_reports/system_usage",
            }).when("/awc_reports/:step", {
                template : "<awc-reports></awc-reports>",
            });
    }]);
