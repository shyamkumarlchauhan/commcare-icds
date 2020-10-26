ICDS Custom SMS
===============================

Complex SMSes sent to beneficiaries and users based on certain criteria.
The information to be sent is computed when the SMS is being sent.

Such SMSes are configured by setting AVAILABLE_CUSTOM_SCHEDULING_CONTENT in localsettings.
Currently set SMSes for ICDS can be found [here](https://github.com/dimagi/icds-commcare-environments/blob/master/environments/icds-cas/public.yml).

Translations
--------------------------------
All SMSes are to be translated and added in [folder](https://github.com/dimagi/commcare-icds/tree/master/custom/icds/templates/icds/messaging/indicators).
SMS is only sent if the SMS is available in user's preferred language.


Aggregated performances SMSes 
--------------------------------
- The content for performance statistics is kept same as what the LS
user would see on her phone. This is done by generating the content that would be sent to the LS user in the restore 
for corresponding statistics.
- Currently we have two versions of the performance SMSes for LS and AWW.<br>
The version to be sent to the user (AWW/LS) is based on the app version in use by the LS user since the information to be used to populate SMS content is picked from what the LS user would see on her phone. The older version is retained for users still using the old version of the applications.<br>
  The new version is suffixed with V2 like `custom.icds.messaging.indicators.AWWAggregatePerformanceIndicatorV2` while the corresponding old version would be `custom.icds.messaging.indicators.AWWAggregatePerformanceIndicator`. The V2 version uses the new UCR alias references and does latest set of calculations.
  Check out `custom.icds.messaging.custom_content._use_v2_indicators` for details on which version is picked for the SMS to be sent.

