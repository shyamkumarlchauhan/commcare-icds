/* global moment */

window.angular.module('icdsApp').factory('dateHelperService', ['$location', function ($location) {
    var reportStartDates = {
        'sdd': new Date(2019, 1),
    };

    var defaultStartYear = 2017;
    var defaultStartMonth = 3;

    function getSelectedMonth() {
        // gets the selected month from $location or defaults to the current month
        // note that this is a 1-indexed month
        return $location.search()['month'] !== void(0) ? parseInt($location.search()['month']) : new Date().getMonth() + 1;
    }
    function getSelectedYear() {
        // gets the selected year from $location or defaults to the current month
        return $location.search()['year'] !== void(0) ? parseInt($location.search()['year']) : new Date().getFullYear();
    }
    function getSelectedDate() {
        // gets the selected date which is the first of the current month, year
        return new Date(getSelectedYear(), getSelectedMonth() - 1, 1);
    }
    function updateSelectedMonth(month, year) {
        $location.search('month', month);
        $location.search('year', year);
    }
    function getSelectedMonthDisplay() {
        var formattedMonth = moment(getSelectedMonth(), 'MM').format('MMMM');
        return formattedMonth + ' ' + getSelectedYear();
    }
    function getCustomAvailableMonthsForReports(selectedYear, selectedMonth, monthsCopy, isSDD) {
        var months = monthsCopy;

        if (selectedYear === new Date().getFullYear()) {
            months = _.filter(monthsCopy, function (month) {
                return month.id <= new Date().getMonth() + 1;
            });
            selectedMonth = selectedMonth <= new Date().getMonth() + 1 ? selectedMonth : new Date().getMonth() + 1;

        } else {

            // Checks if its Service delivery dashboard and the selected year is 2019 then its month list
            // should start from Feb
            if (isSDD && selectedYear === reportStartDates['sdd'].getFullYear()) {
                months = _.filter(monthsCopy, function (month) {
                    return month.id >= reportStartDates['sdd'].getMonth() + 1;
                });

                selectedMonth = selectedMonth >= reportStartDates['sdd'].getMonth() + 1 ?
                    selectedMonth : reportStartDates['sdd'].getMonth() + 1;
            }

            // Dashboard data is available from 2017 March
            if (selectedYear === 2017) {
                months = _.filter(monthsCopy, function (month) {
                    return month.id >= 3;
                });
                selectedMonth = selectedMonth >= 3 ? selectedMonth : 3;
            }

        }
        return {
            'months': months,
            'selectedMonth': selectedMonth,
        };

    }
    function getStartingMonth(isSDD) {
        if (isSDD) {
            return reportStartDates['sdd'].getMonth() + 1;
        }
        return defaultStartMonth;
    }
    function getStartingYear(isSDD) {
        if (isSDD) {
            return reportStartDates['sdd'].getFullYear();
        }
        return defaultStartYear;
    }
    function getReportStartDates() {
        return reportStartDates;
    }
    return {
        getSelectedMonth: getSelectedMonth,
        getSelectedYear: getSelectedYear,
        getSelectedDate: getSelectedDate,
        getSelectedMonthDisplay: getSelectedMonthDisplay,
        updateSelectedMonth: updateSelectedMonth,
        getCustomAvailableMonthsForReports: getCustomAvailableMonthsForReports,
        getStartingYear: getStartingYear,
        getStartingMonth: getStartingMonth,
        getReportStartDates: getReportStartDates,
    };
}]);
