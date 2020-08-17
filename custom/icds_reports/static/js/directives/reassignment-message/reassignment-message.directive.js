function ReassignmentMessageController($scope) {
    var vm = this;
    vm.$onChanges = function(){
        if (vm.selectedLocation) {
            vm.archived_on = vm.selectedLocation.archived_on;
            vm.deprecated_to = vm.selectedLocation.deprecated_to;
            vm.name = vm.selectedLocation.name;
            vm.deprecates_at = vm.selectedLocation.deprecates_at;
            vm.deprecates = vm.selectedLocation.deprecates;
        }
    }
}

ReassignmentMessageController.$inject = ['$scope',]

window.angular.module('icdsApp').component("reassignmentMessage", {
    bindings: {
        selectedLocation: '<',
        selectedDate: '<',
    },
    templateUrl: ['templateProviderService', function (templateProviderService) {
        return templateProviderService.getTemplate('reassignment-message');
    }],
    controller: ReassignmentMessageController,
});
