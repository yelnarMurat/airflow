SELECT  fa.shift_startdate_local_id    
      , fa.schedule_date_id    
      , fa.person_id    
      , fa.interval_id    
      , fa.activity_starttime    
      , fa.scenario_id    
      , fa.activity_id    
      , fa.absence_id    
      , fa.activity_startdate_id    
      , fa.activity_enddate_id    
      , fa.activity_endtime    
      , fa.shift_startdate_id    
      , fa.shift_starttime    
      , fa.shift_enddate_id    
      , fa.shift_endtime    
      , fa.shift_startinterval_id    
      , fa.shift_endinterval_id    
      , fa.shift_category_id    
      , fa.shift_length_id    
      , fa.scheduled_time_m    
      , fa.scheduled_time_absence_m    
      , fa.scheduled_time_activity_m    
      , fa.scheduled_contract_time_m    
      , fa.scheduled_contract_time_activity_m scheduled_contract_time_activi    
      , fa.scheduled_contract_time_absence_m scheduled_contract_time_absenc    
      , fa.scheduled_work_time_m    
      , fa.scheduled_work_time_activity_m    
      , fa.scheduled_work_time_absence_m    
      , fa.scheduled_over_time_m    
      , fa.scheduled_ready_time_m    
      , fa.scheduled_paid_time_m    
      , fa.scheduled_paid_time_activity_m    
      , fa.scheduled_paid_time_absence_m    
      , fa.business_unit_id    
      , fa.datasource_id    
      , fa.insert_date    
      , fa.update_date    
      , fa.datasource_update_date    
      , fa.overtime_id    
      , fa.planned_overtime_m    
  FROM [fact_schedule] fa with (NOLOCK)    
  where [shift_startdate_local_id]%1000=0
