UPDATE icds_reports_aggregateinactiveaww ac
    SET awc_site_code = loc.awc_site_code
    FROM awc_location_local loc
    WHERE loc.doc_id = ac.awc_id
    AND loc.aggregation_level = 5;
