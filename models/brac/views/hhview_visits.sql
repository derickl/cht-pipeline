SELECT 
	upr.reported AS reported,
	upr.reported_by_parent,
	cvp.parent_uuid AS household_id
FROM 
	{{ ref("useview_patient_record") }} AS upr
INNER JOIN {{ ref("contactview_person") }} cvp ON (upr.patient_id=cvp.patient_id)
UNION 
SELECT 
	reported,
	reported_by_parent,
	place_id AS household_id
FROM 
	{{ ref("useview_place_record") }}
UNION
SELECT 
	reported::timestamp as reported,
	parent_uuid AS reported_by_parent,
	uuid AS household_id
FROM 
	{{ ref("contactview_metadata") }} 
WHERE 
	type='clinic'