T_SUBSCRIPTION_HELPER_QUERY = """
SELECT 
  sub.subscriptionid as subscriptionID, 
  cus.masteraccountid as masterAccountID,
  sub.recordcreatedat as recordCreatedAt,
  sub.clientid as clientId,
  sub.crmsource as crmSource,
  sub.preferredDays,
  sub.preferredStart,
  sub.preferredEnd,
  sub.recurringTicket,
  sub.annualRecurringValue,
  sub.annualRecurringServices,
  lkp.isRecurring as includedContract,
  CASE 
    WHEN sub.active = 1 THEN true
    ELSE false
  END AS isActive,
  CASE 
      WHEN sub.active = 1 THEN TIMESTAMP("2199-01-01 00:00:00")
      ELSE CAST(dateCancelled AS TIMESTAMP)
  END AS adjEndDate,
  -- Check if the subscription ends after the reference date
  CASE 
    WHEN sub.dateCancelled IS NULL or sub.active=1 THEN TRUE
    WHEN sub.dateCancelled > (
      SELECT SAFE_CAST(value AS TIMESTAMP) 
      FROM `pco-qa.raw_layer.lkp_report_type` 
      WHERE reportType = 'referenceDate' 
        AND clientId = sub.clientId 
        AND crmSource = sub.crmSource
    ) THEN TRUE
    ELSE FALSE
  END AS endAfterRefDate,

  -- Check if the subscription starts on or before the reference date
  CASE
    WHEN sub.dateAdded IS NULL THEN FALSE
    WHEN sub.dateAdded <= (
      SELECT SAFE_CAST(value AS TIMESTAMP) 
      FROM `pco-qa.raw_layer.lkp_report_type` 
      WHERE reportType = 'referenceDate' 
        AND clientId = sub.clientId 
        AND crmSource = sub.crmSource
    ) THEN TRUE
    ELSE FALSE
  END AS startOnOrBeforeRefDate,
  lkp.allocatereservices as allocateReservice,
FROM `pco-qa.transformation_layer.merged_subscription` sub
LEFT JOIN `pco-qa.transformation_layer.merged_customer` cus 
  ON sub.individualaccountid = cus.individualaccountid 
  AND sub.clientid = cus.clientid 
  AND sub.crmsource = cus.crmsource
left join `pco-qa.raw_layer.lkp_service_type` lkp on lkp.servicetype = sub.serviceid and lkp.clientid = sub.clientid and lkp.crmsource = sub.crmsource
WHERE sub.clientId = @clientId
"""

T_APPOINTMENT_HELPER_QUERY = """
SELECT 
    app.appointmentID,
    app.individualAccountID,
    app.servicedBy,
    CAST(cus.masterAccountID as Int64) as masterAccountID,
    tic.prodFromTicket,
    app.timeIn,
    app.timeOut,
    app.appointmentDate,
    app.type,
    app.isInitial,
    app.status,
    app.duration,
    map.groupForMultivisit,
    svt.isRecurring as isRecurring,
    svt.allocateReservices as allocateReservices,
    -- Updated isRervice logic with toggle
    CASE 
        WHEN toggle.value = '1' AND tic.subTotal = 0 THEN TRUE
        ELSE svt.isRervice
    END as isRervice,
    svt.zeroVisitTime as zeroVisitTime,
    app.appointmentDate AS computedAppointmentDate,
    DATE_SUB(SAFE_CAST(lkp.value AS DATE), INTERVAL 2 YEAR) AS twoYearsBefore,
    SAFE_CAST(lkp.value AS DATE) AS reference_date,
    CASE 
        WHEN COALESCE(DATE(app.appointmentDate), DATE(app.timeIn)) >= DATE_SUB(SAFE_CAST(lkp.value AS DATE), INTERVAL 2 YEAR)
        AND COALESCE(DATE(app.appointmentDate), DATE(app.timeIn)) < SAFE_CAST(lkp.value AS DATE)
        THEN TRUE 
        ELSE FALSE 
    END AS inRefPeriod,
    CASE
     WHEN app.status = 1 THEN 
      (UNIX_SECONDS(app.timeOut) - UNIX_SECONDS(app.timeIn)) / 60.0
        ELSE 0.0
    END AS crmMinutes,
    -- Additional columns
    app.recordCreatedAt,
    app.clientId,
    app.crmSource

FROM `pco-qa.transformation_layer.merged_appointment` app
LEFT JOIN `pco-qa.transformation_layer.merged_customer` cus 
    ON app.individualAccountID = cus.individualAccountID 
    AND app.clientId = cus.clientId 
    AND app.crmSource = cus.crmSource
LEFT JOIN `pco-qa.raw_layer.lkp_report_type` lkp 
    ON app.clientId = lkp.clientId 
    AND app.crmSource = lkp.crmSource
    AND lkp.reportType = 'referenceDate'
-- Add toggle lookup
LEFT JOIN `pco-qa.raw_layer.lkp_report_type` toggle
    ON app.clientId = toggle.clientId 
    AND app.crmSource = toggle.crmSource
    AND toggle.reportType = 'toggle'
LEFT JOIN `pco-qa.raw_layer.lkp_master_account_map` map
    ON app.individualAccountID = map.customerID 
    AND app.clientId = map.clientID
    AND app.crmSource = map.crmSource
LEFT JOIN `pco-qa.raw_layer.lkp_service_type` svt
on svt.clientId = app.clientId
and svt.crmSource = app.crmSource
and app.type = svt.serviceType
left join `pco-qa.transformation_layer.merged_ticket` tic on app.ticketID = tic.ticketID
and app.clientId = tic.clientId and app.crmSource = tic.crmSource
WHERE app.clientId = @clientId and appointmentDate < SAFE_CAST(lkp.value AS DATE) and appointmentDate >= DATE_SUB(SAFE_CAST(lkp.value AS DATE), INTERVAL 2 YEAR)
"""

T_SUBSCRIPTION_HELPER = """pco-qa.transformation_layer.t_subscription_helper"""
T_APPOINTMENT_HELPER = """pco-qa.transformation_layer.t_appointment_helper"""
DISTINCT_CLIENT_SUBSCRIPTION = """SELECT DISTINCT clientId from pco-qa.transformation_layer.merged_subscription order by clientId"""
DISTINCT_CLIENT_APPOINTMENT = (
    """SELECT DISTINCT clientId from pco-qa.transformation_layer.merged_appointment order by clientId"""
)
CLIENT_ID = "clientId"
STRING = "STRING"
TRUNCATE_T_SUBSCRIPTION_HELPER = (
    "DELETE FROM pco-qa.transformation_layer.t_subscription_helper WHERE clientId = @clientId"
)
TRUNCATE_T_APPOINTMENT_HELPER = (
    "DELETE FROM pco-qa.transformation_layer.t_appointment_helper WHERE clientId = @clientId"
)
