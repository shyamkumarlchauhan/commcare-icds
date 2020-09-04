hqDefine("icds_reports/js/base", function () {
    $(function () {
        $(document).on('change', '#fieldset_location_async select, #fieldset_month select, #fieldset_year select', function (e) {
            e.stopPropagation();
            var applyBtn = $('#apply-filters');

            var mprLocationInfo = $('#mpr-banner-info');
            var isMprReport = mprLocationInfo.length === 1;

            if (isMprReport) {
                var isStateOrBelowSelected = $('select.form-control').length >= 2;
                setTimeout(function () {
                    if (isStateOrBelowSelected) {
                        applyBtn.enableButton();
                        mprLocationInfo.hide();
                    } else {
                        applyBtn.disableButtonNoSpinner();
                        mprLocationInfo.show();
                    }
                }, 0);
            } else {
                var state = $('select.form-control')[0].selectedIndex;
                if (state === 0) {
                    applyBtn.disableButtonNoSpinner();
                } else {
                    applyBtn.enableButton();
                }
            }

        });
    });
});
