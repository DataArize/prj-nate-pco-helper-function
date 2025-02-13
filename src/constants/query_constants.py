T_SUBSCRIPTION_HELPER_QUERY = """
SELECT 
  sub.subscriptionid, 
  cus.masteraccountid,
  sub.recordcreatedat,
  sub.clientid,
  sub.crmsource,
  sub.preferredDays,
  sub.preferredStart,
  sub.preferredEnd,
  sub.annualRecurringValue,
  -- Check if the subscription ends after the reference date
  CASE 
    WHEN sub.dateCancelled IS NULL THEN TRUE
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
    WHEN sub.dateAdded <= (
      SELECT SAFE_CAST(value AS TIMESTAMP) 
      FROM `pco-qa.raw_layer.lkp_report_type` 
      WHERE reportType = 'referenceDate' 
        AND clientId = sub.clientId 
        AND crmSource = sub.crmSource
    ) THEN TRUE
    ELSE FALSE
  END AS startOnOrBeforeRefDate,

  -- Logical AND condition for endAfterRefDate AND startOnOrBeforeRefDate
  CASE 
    WHEN 
      ( 
        (sub.dateCancelled IS NULL OR sub.dateCancelled > (
          SELECT SAFE_CAST(value AS TIMESTAMP) 
          FROM `pco-qa.raw_layer.lkp_report_type` 
          WHERE reportType = 'referenceDate' 
            AND clientId = sub.clientId 
            AND crmSource = sub.crmSource
        )) 
        AND
        (sub.dateAdded <= (
          SELECT SAFE_CAST(value AS TIMESTAMP) 
          FROM `pco-qa.raw_layer.lkp_report_type` 
          WHERE reportType = 'referenceDate' 
            AND clientId = sub.clientId 
            AND crmSource = sub.crmSource
        ))
      ) 
    THEN TRUE 
    ELSE FALSE 
  END AS isActive,
  lkp.allocatereservices as allocateReservice,
FROM `pco-qa.transformation_layer.merged_subscription` sub
LEFT JOIN `pco-qa.transformation_layer.merged_customer` cus 
  ON sub.individualaccountid = cus.individualaccountid 
  AND sub.clientid = cus.clientid 
  AND sub.crmsource = cus.crmsource
left join `pco-qa.raw_layer.lkp_service_type` lkp on lkp.servicetype = sub.serviceid and lkp.clientid = sub.clientid and lkp.crmsource = sub.crmsource
"""

T_APPOINTMENT_HELPER_QUERY = """
SELECT 
    app.appointmentID,
    CAST(cus.masterAccountID as Int64) as masterAccountID,
    app.timeIn,
    app.timeOut,
    app.appointmentDate,
    app.type,
    app.isInitial,
    app.status,
    app.duration,
    svt.isRecurring as isRecurring,
    svt.allocateReservices as allocateReservices,
    svt.isRervice as isRervice,
    COALESCE(DATE(app.appointmentDate), DATE(app.timeIn)) AS computedAppointmentDate,
    DATE_SUB(SAFE_CAST(lkp.value AS DATE), INTERVAL 2 YEAR) AS twoYearsBefore,
    SAFE_CAST(lkp.value AS DATE) AS reference_date,
    CASE 
        WHEN COALESCE(DATE(app.appointmentDate), DATE(app.timeIn)) >= DATE_SUB(SAFE_CAST(lkp.value AS DATE), INTERVAL 2 YEAR)
        AND COALESCE(DATE(app.appointmentDate), DATE(app.timeIn)) < SAFE_CAST(lkp.value AS DATE)
        THEN TRUE 
        ELSE FALSE 
    END AS inRefPeriod,
    CASE
      WHEN app.status = 1 THEN TIMESTAMP_DIFF(app.timeOut, app.timeIn, MINUTE)
      ELSE 0.0
    END as crmMinutes,
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
LEFT JOIN `pco-qa.raw_layer.lkp_service_type` svt
on svt.clientId = app.clientId
and svt.crmSource = app.crmSource
and app.type = svt.serviceType
"""

T_SUBSCRIPTION_HELPER = """pco-qa.transformation_layer.t_subscription_helper"""
T_APPOINTMENT_HELPER = """pco-qa.transformation_layer.t_appointment_helper"""
