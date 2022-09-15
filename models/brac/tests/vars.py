from dbsettings import *

queries = {}
queries_2 = {}

# UnComment queries to run as necessary

# queries["get_hmis_data"]= """
#     SELECT t.chp_uuid, t.date, md5((ROW(t.*)::TEXT)) as hash
#     FROM get_hmis_data('2021-01-01', '2021-03-31') t ;
# """

# queries_2["get_hmis_data"]= """
#     SELECT t.chp_uuid, t.date, md5((ROW(t.*)::TEXT)) as hash
#     FROM dbt.get_hmis_data('2021-01-01', '2021-03-31') t;
# """

# queries["get_assessment_data"]= """
#     SELECT t.chw_uuid, t.month, md5((ROW(t.*)::TEXT)) as hash
#     FROM get_assessment_data('2021-01-01', '2021-03-31') t ;
# """

# queries_2["get_assessment_data"]= """
#     SELECT t.chw_uuid, t.month, md5((ROW(t.*)::TEXT)) as hash
#     FROM dbt.get_assessment_data('2021-01-01', '2021-03-31') t;
# """

queries["get_dashboard_data"]= """
    SELECT t.chw_uuid, t.interval_start, md5((ROW(t.*)::TEXT)) as hash
    FROM get_dashboard_data('branch', '2021-01-01', '2021-03-31', 'false') t ;
"""

queries_2["get_dashboard_data"]= """
    SELECT t.chw_uuid, t.interval_start, md5((ROW(t.*)::TEXT)) as hash
    FROM dbt.get_dashboard_data('branch', '2021-01-01', '2021-03-31', 'false') t;
"""



def search_hash(needle, arrayHaystack, foundCounter):
    for index, item in enumerate(arrayHaystack):
        if item[2] == needle:
            print("Found: ", needle, "Index: ", index)
            foundCounter+=1
            del arrayHaystack[index]
            break

    return arrayHaystack, foundCounter



            