hqDefine("icds_reports/js/update_manual", [
    'jquery',
    'reports/js/filters/main',
], function ($, filtersMain) {
    $(function () {
        filtersMain.init();
    });
});