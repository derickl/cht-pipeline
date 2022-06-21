{{
    config(
        materialized = 'incremental',
        unique_key='chwview_assessment_chw_uuid_date',
        indexes=[
            {'columns': ['chw_uuid']},
            {'columns': ['"@timestamp"']},
            {'columns': ['chw_uuid', 'reported_day'], 'type': 'hash'} 
        ]
    )
}}

	SELECT 
    "@timestamp"::timestamp without time zone AS "@timestamp",
	form.doc #>> '{contact,_id}'::text[] AS chw_uuid,
    date_trunc('day'::text, '1970-01-01 00:00:00'::timestamp without time zone + ((form.doc ->> 'reported_date'::text)::bigint)::double precision * '00:00:00.001'::interval) AS reported_day,
   count(*) AS assess_any,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 1 THEN 1
           ELSE 0
       END) AS assess_u1,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 5 THEN 1
           ELSE 0
       END) AS assess_u5,
   sum(
       CASE
           WHEN (form.doc #>> '{fields,group_diagnosis,diagnosis_fever}'::text[]) ~~ 'malaria%'::text THEN 1
           ELSE 0
       END) AS malaria_all_ages,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 1 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_fever}'::text[]) ~~ 'malaria%'::text THEN 1
           ELSE 0
       END) AS malaria_u1,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 1 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_diarrhea}'::text[]) ~~ 'diarrhea%'::text THEN 1
           ELSE 0
       END) AS diarrhea_u1,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 1 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_cough}'::text[]) ~~ 'pneumonia%'::text THEN 1
           ELSE 0
       END) AS pneumonia_u1,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 5 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_fever}'::text[]) ~~ 'malaria%'::text THEN 1
           ELSE 0
       END) AS malaria_u5,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 5 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_diarrhea}'::text[]) ~~ 'diarrhea%'::text THEN 1
           ELSE 0
       END) AS diarrhea_u5,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 5 AND (form.doc #>> '{fields,group_diagnosis,diagnosis_cough}'::text[]) ~~ 'pneumonia%'::text THEN 1
           ELSE 0
       END) AS pneumonia_u5,
   sum(
       CASE
           WHEN (form.doc #>> '{fields,group_fever,patient_fever}'::text[]) = 'yes'::text AND (form.doc #>> '{fields,group_fever,mrdt_result}'::text[]) = 'positive'::text THEN 1
           ELSE 0
       END) AS mrdt_positive,
   sum(
       CASE
           WHEN (form.doc #>> '{fields,group_fever,patient_fever}'::text[]) = 'yes'::text AND (form.doc #>> '{fields,group_fever,mrdt_result}'::text[]) = 'negative'::text THEN 1
           ELSE 0
       END) AS mrdt_negative,
   sum(
       CASE
           WHEN (form.doc #>> '{fields,group_fever,patient_fever}'::text[]) = 'yes'::text AND (form.doc #>> '{fields,group_fever,mrdt_result}'::text[]) = 'none'::text THEN 1
           ELSE 0
       END) AS mrdt_none,
   sum(
       CASE
           WHEN ((form.doc #>> '{fields,patient_age_in_years}'::text[])::integer) < 5 AND (form.doc #>> '{fields,referral_follow_up}'::text[]) = 'true'::text THEN 1
           ELSE 0
       END) AS required_follow_ups
  FROM {{ ref("couchdb") }} form
 WHERE (form.doc ->> 'type'::text) = 'data_record'::text AND form.doc ? 'form'::text AND (form.doc ->> 'form'::text) = 'assessment'::text
 {% if is_incremental() %}
            AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
 GROUP BY 
    "@timestamp"::timestamp without time zone,
 	form.doc #>> '{contact,_id}'::text[], 
 	date_trunc('day'::text, '1970-01-01 00:00:00'::timestamp without time zone + ((form.doc ->> 'reported_date'::text)::bigint)::double precision * '00:00:00.001'::interval);