
function NavMenuOpenerController($scope) {
    $scope.openNavMenu = function () {
        // todo: this is better implemented with angular events rather
        // than magic div ID names...
        document.getElementById('nav-menu').style.left = '0';
    };
}

NavMenuOpenerController.$inject = ['$scope'];

window.angular.module('icdsApp').directive("navMenuOpener",  ['templateProviderService', function (templateProviderService) {
    return {
        restrict:'E',
        bindToController: true,
        templateUrl: function () {
            return templateProviderService.getTemplate('nav-menu-opener.directive');
        },
        controller: NavMenuOpenerController,
    };
}]);
