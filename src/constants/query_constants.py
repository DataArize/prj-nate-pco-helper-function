T_SUBSCRIPTION_HELPER_QUERY = """
SELECT 
  sub.subscriptionID, 
  cus.masterAccountID
  cus.isCommercial as commercial,
  sub.preferredDays,
  sub.preferredStart,
  sub.preferredEnd,
  lkp.allocateReservices as reserviceableContract,
  lkp.isRecurring as includedContract,
  CASE 
      WHEN sub.active = 1 THEN TIMESTAMP("2199-01-01 00:00:00")
      ELSE CAST(dateCancelled AS TIMESTAMP)
    END AS adjEndDate,
  lkp.majorRecurringBucket as majorBucket,
  lkp.minorRecurringBucket as minorBucket,
  CASE
    WHEN lkp.majorRecurringBucket = 'Residential - General Pest' then TRUE
    else FALSE
  END AS residentialGeneralPest,
  CASE
    WHEN lkp.majorRecurringBucket = 'Commercial - General Pest' then TRUE
    else FALSE
  END as commercialGeneralPest,
  CASE
    WHEN lkp.majorRecurringBucket = 'Wood Destroying' then TRUE
    else FALSE
  END as woodDestroying,
  CASE
    WHEN lkp.majorRecurringBucket = 'Mosquito' then TRUE
    else FALSE
  END as mosquito,
  CASE
    WHEN lkp.majorRecurringBucket = 'Other' then TRUE
    else FALSE
  END as other,
  sub.recordCreatedAt,
  sub.clientId,
  sub.crmSource
FROM `pco-qa.transformation_layer.merged_subscription` sub
left join `pco-qa.transformation_layer.merged_customer` cus on sub.individualAccountID = cus.individualAccountID and sub.clientId = cus.clientId and sub.crmSource = cus.crmSource
left join `pco-qa.raw_layer.lkp_service_type` lkp on lkp.serviceType = sub.serviceID 
"""

T_APPOINTMENT_HELPER_QUERY = """
SELECT app.appointmentID,
    cus.masterAccountID,
    tic.prodFromTicket,
    app.appointmentDate,
    app.status,
    app.duration,
    app.timeIn,
    app.timeOut,
    CASE 
            WHEN tic.PRODUCTION_VALUE = '-1' THEN tic.SUB_TOTAL
            WHEN tic.PRODUCTION_VALUE = '-1.00' THEN tic.SUB_TOTAL
            ELSE tic.PRODUCTION_VALUE
    END AS prodFromTicket,
    CASE
      WHEN status = 1 THEN TIMESTAMP_DIFF(timeOut, timeIn, MINUTE)
      ELSE 0.0
    END as crmMinutes,
    CASE 
        WHEN app.type = 3 then (SELECT SAFE_CAST(value AS FLOAT64) FROM `pco-qa.raw_layer.lkp_time_assumption` WHERE timeAssumption='Reservice: Paid Drive Time Ratio')
        ELSE (SELECT SAFE_CAST(value AS FLOAT64) FROM `pco-qa.raw_layer.lkp_time_assumption` WHERE timeAssumption='Avg. Drive Minutes Paid')
    END AS value,
    lkp.AverageMinutes,
    lkp.serviceTypeName as serviceTypeText,
    lkp.isRervice as isReservice,
    lkp.isRecurring as includedType,
    lkp.allocateReservices as countForReservice,
    CASE
        WHEN lkp.majorRecurringBucket = 'Residential - General Pest' then TRUE
        else FALSE
    END AS residentialGreenPest,
      CASE
        WHEN lkp.majorRecurringBucket = 'Commercial - General Pest' then TRUE
        else FALSE
      END as commercialGreenPest,
      CASE
        WHEN lkp.majorRecurringBucket = 'Wood Destroying' then TRUE
        else FALSE
      END as woodDestroying,
      CASE
        WHEN lkp.majorRecurringBucket = 'Mosquito' then TRUE
        else FALSE
      END as mosquito,
      CASE
        WHEN lkp.majorRecurringBucket = 'Other' then TRUE
        else FALSE
      END as other,
      CASE
        WHEN lkp.majorRecurringBucket = 'Residential - basic' then TRUE
        else FALSE
      END as residentialBasic,
      CASE
        WHEN lkp.majorRecurringBucket = 'Residential - Premium' then TRUE
        else FALSE
      END as residentialPremium,
      CASE
        WHEN lkp.majorRecurringBucket = 'Residential - Plus' then TRUE
        else FALSE
      END as residentialPlus,
      app.recordCreatedAt,
      app.clientId,
      app.crmSource
FROM `pco-qa.transformation_layer.merged_appointment`  app
left join `pco-qa.raw_layer.lkp_service_type` as lkp on lkp.serviceType = app.type
left join `pco-qa.transformation_layer.merged_customer` cus on app.individualAccountID = cus.individualAccountID and app.clientId = cus.clientId and app.crmSource = cus.crmSource
left join `pco-qa.transformation_layer.merged_ticket` tic on app.ticketID = tic.ticketID and app.clientId = tic.clientId and app.crmSource = tic.crmSource
"""

T_SUBSCRIPTION_HELPER = """pco-qa.transformation_layer.t_subscription_helper"""
T_APPOINTMENT_HELPER = """pco-qa.transformation_layer.t_appointment_helper"""
# WHERE_CONDITION = " WHERE sub.recordCreatedAt > @max_timestamp"
TIMESTAMP = "TIMESTAMP"
MAX_TIMESTAMP = "max_timestamp"
