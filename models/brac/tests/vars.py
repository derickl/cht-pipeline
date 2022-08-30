from dbsettings import *

queries = {}
queries_2 = {}
# queries["ancview_pregnancy"] = """
# SELECT a.random_uuid, b.hash, c.hash FROM
# (
#     SELECT t.uuid as random_uuid FROM ancview_pregnancy t TABLESAMPLE SYSTEM_ROWS (1)
# ) as a

# LEFT JOIN 
# (
# SELECT uuid,  md5((ROW(f.reported_by, f.reported)::TEXT)) as hash
# FROM ancview_pregnancy f 
# ) as b 
# ON b.uuid = a.random_uuid
# LEFT JOIN
# (
# SELECT i.uuid, i.hash FROM dblink('dbname=cht_pipeline_test port=5432 host={host} user={user} password={password}', 
#                                 'SELECT uuid, md5((ROW(f.reported_by, f.reported)::TEXT)) as hash 
#                                  FROM dbt_clone.ancview_pregnancy f
#                                  '
#                          ) 
# AS i (uuid TEXT, hash TEXT)
# ) c
# ON b.uuid = c.uuid;""".format(host=host, user=user, password=password)

queries["get_hmis_data"]= """
    SELECT t.chp_uuid, t.date, md5((ROW(t.*)::TEXT)) as hash
    FROM get_hmis_data('2021-01-01', '2021-01-31') t ;
"""

queries_2["get_hmis_data"]= """
    SELECT t.chp_uuid, t.date, md5((ROW(t.*)::TEXT)) as hash
    FROM dbt_clone.get_hmis_data('2021-01-01', '2021-01-31') t;
"""

# queries["get_assessment_data"]= """
# SELECT a.chw_uuid, a.hash, b.hash FROM(
# SELECT i.chw_uuid, i.month, i.hash FROM dblink('dbname=cht_pipeline_test port=5432 host={host} user={user} password={password}', 
#         'SELECT 
#         f.chw_uuid, 
#         f.month, 
#         md5((ROW(f.assess_any, f.assess_u1, f.assess_u5)::TEXT)) as hash
#         FROM dbt_clone.get_assessment_data(''2021-03-01'', ''2021-03-03'') f 
#         offset random() * (select count(*) from dbt_clone.get_assessment_data(''2021-03-01'', ''2021-03-03'')) limit 1
#         '
#         )
# AS i (chw_uuid TEXT, month TEXT, hash TEXT)
# ) AS a
# JOIN 
# (
# SELECT * FROM
# (
#     SELECT t.chw_uuid, t.month, md5((ROW(t.assess_any, t.assess_u1, t.assess_u5)::TEXT)) as hash
#     FROM get_assessment_data('2021-03-01', '2021-03-03') t 
# ) as x
# ) as b
# ON b.chw_uuid = a.chw_uuid AND b.month = a.month
# """.format(host=host, user=user, password=password)

# queries["get_dashboard_data"]= """
# SELECT a.chw_uuid, a.hash, b.hash FROM(
# SELECT i.chw_uuid, i.interval_start, i.hash FROM dblink('dbname=cht_pipeline_test port=5432 host={host} user={user} password={password}', 
#         'SELECT 
#         f.chw_uuid, 
#         f.interval_start, 
#         md5((ROW(f.pregnancies_registered, f.fp_visits, f.families_registered)::TEXT)) as hash
#         FROM dbt_clone.get_dashboard_data(''branch'', ''2021-03-01'', ''2021-03-03'', ''true'') f 
#         offset random() * (select count(*) from dbt_clone.get_dashboard_data(''branch'', ''2021-03-01'', ''2021-03-03'', ''true'')) limit 1
#         '
#         )
# AS i (chw_uuid TEXT, interval_start TEXT, hash TEXT)
# ) AS a
# JOIN 
# (
# SELECT * FROM
# (
#     SELECT t.chw_uuid, t.interval_start, md5((ROW(t.pregnancies_registered, t.fp_visits, t.families_registered)::TEXT)) as hash
#     FROM get_dashboard_data('branch', '2021-03-01', '2021-03-03', 'true') t 
# ) as x
# ) as b
# ON b.chw_uuid = a.chw_uuid AND b.interval_start::TEXT = a.interval_start::TEXT and a.hash = b.hash
# LIMIT 1
# """


def search_hash(needle, arrayHaystack, foundCounter):
    for index, item in enumerate(arrayHaystack):
        if item[2] == needle:
            print("Found: ", needle, "Index: ", index)
            foundCounter+=1
            del arrayHaystack[index]

    return arrayHaystack, foundCounter



            