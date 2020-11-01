Location Reassignment
===============================

Location Reassignment provides a safe way to modify the location structure of ICDS. Although changing locations should be easy but few factors make this challenging.

On CommCareHQ
1. A mobile user's username is unique and can never be changed.
2. A location's site code is unique for locations (including archived locations) but can be changed.

For ICDS
1. Location Site Code is "mostly" expected to be same as the username of the mobile worker assigned to that location, depending on the level that location is.
   For example, It is same for AWW but can be different for block/district level users.
2. For ICDS, the site code is same as the LGD code and they are maintained as such that the LGD code of the location is formed from the LGD code of the ancestor locations. So if the site code or the LGD code of any location changes, the site code of all the descendant locations would need to be changed.

So new locations can’t reuse site codes from old locations and hence would always need new site codes.

Keeping all these factors in mind, changing location structure becomes complicated, and hence we use "location reassignment" where an existing location is "reassigned" as new location(s).

Additional Considerations:
1. Cases are owned by locations.
2. Dashboard data is maintained on monthly basis, so any change to location structure should be done before the month begins i.e before the aggregation is done on 1st of the month.

Hence, to perform location reassignment
1. We don’t delete/update locations/users but just mark the locations as deprecated (different from archived) and and create a new location in the requested spot in the hierarchy. "Extract" operation which is explained later is a special case where the original location is not deprecated.<br/>
   Similarly, the users assigned to the old locations are deactivated and new ones are created and assigned to the new locations.<br/>
   If a location is reassigned, all of it's descendants should be considered too.
2. from data perspective, reassign cases from old to new locations.<br/>
   Forms submitted are NOT moved. This is due to feasibility challenges and in order to keep data updates to minimal. This also then helps to the history of submissions unaltered.

Supported workflows:
-------------------------------
The following operations can be performed on a location
1. Move : A location is moved to a new spot in the hierarchy
2. Merge : Two or more locations are merged into one
3. Split : A location is split in two or more locations
4. Extract : One or more locations are extracted out of a location. This is a special case and is different from "Split" in a manner that the original location still continues to exist.

So, a "transition" is an "operation" performed on a location.
Check out `custom.icds.location_reassignment.models.Transition` to understand how a transition is executed.

Process:
-------------------------------
Location reassignment is a major process and needs strong planning.
On UI there are two pages
1. Download Only view `custom.icds.location_reassignment.views.LocationReassignmentDownloadOnlyView` <br/>
   visible to the states to be able to download a template, which is simply an excel file, which they can fill to submit a "request" for location reassignment
2. The main view `custom.icds.location_reassignment.views.LocationReassignmentView` <br/>
   Is accessible only to users that can edit locations where the user can
   - Validate: ensure that the request is valid.<br/>HQ would parse the filled excel sheet and share any discrepancies. HQ validates as much as it can, so a manual validation should be done as well.<br/>It also shares a "Summary" file to summarize what it understands as the transitions to be performed.<br/>Additionally, it would also share the number of cases that would need to be reassigned to hint about the scale of the change.
   - Email household: Household case type is the top most parent of the case hierarchy for ICDS and is more relevant to the AWW app. Household case and cases under it, would be the majority of cases assigned to users.<br/>This option emails details of household cases assigned to the locations that are to be reassigned. Additional details like names of household members is also shared.<br/>This is needed for Split/Extract operations where HQ needs to know what locations to reassign relevant cases too.
   - Email other cases: email basic details of any other type of cases that would need to be reassigned.<br/>This is needed for Split/Extract operations where HQ needs to know what locations to reassign relevant cases too.
   - Update: Perform the actual location reassignment.<br/>This would create any missing locations, deprecate locations and perform necessary operations related to locations. It would also initiate asynchronous reassignment of cases.
   - Reassign Household cases: perform the reassignment of household and it's child cases like person cases, based on the sheet filled from "Email households"
   - Reassign other cases: perform the reassignment of other cases based on the sheet filled from "Email other cases"

From Dashboard perspective, it is imperative that all operations and case reassignments are finished before the aggregation is run on 1st of the month.
It's also essential that UCRs have been updated as well.

Overall the process is to be started only after the aggregation has finished on the day before 1st and should finish before the aggregation is run on 1st.<br/>
The operations can take a long time to finish based on the number of locations and cases to be reassigned.<br/>
Only if needed, the aggregation can be skipped on 1st giving HQ more time to finish the location reassignment or if the operations fail to execute and need to be fixed. Once location reassignment is initiated, it CAN NOT be rolled back.

Code References
-------------------------------
1. `custom.icds.location_reassignment.parser.Parser` validates the location reassignment request
2. `custom.icds.location_reassignment.models.Transition` encapsulates all actions that are to be done as a part of the transition.
3. `custom.icds.location_reassignment.processor` contains all the classes that handle the responsibility of performing real requests.
4. `custom.icds.location_reassignment.tasks.process_ucr_changes` 