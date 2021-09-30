import os
import sys
import ibmcloudsql
import pandas as pd

if 'APIKEY' not in os.environ:
	print("Error: Variable APIKEY must be defined.")
	sys.exit(1)
if 'SQL_INSTANCE_CRN' not in os.environ:
        print("Error: Variable SQL_INSTANCE_CRN must be defined.")
        sys.exit(1)
if 'RESULT_LOCATION' not in os.environ:
        print("Error: Variable RESULT_LOCATION must be defined.")
        sys.exit(1)

sqlClient = ibmcloudsql.SQLQuery(os.environ['APIKEY'], os.environ['SQL_INSTANCE_CRN'], os.environ['RESULT_LOCATION'], client_info='iot-compactor')
sqlClient.logon()

compactable=sqlClient.run_sql("select * from compactable_data")   

if compactable.compactable_hours[0] > 0:
	print("Compacting {} hours of data between {} and {} (UTC) ...".format(compactable.compactable_hours[0], compactable.compacted_hour_threshold[0], compactable.last_full_hour_threshold[0]))
else:
	print("Nothing to compact. Lastest compaction threshold is {}.".format(compactable.compacted_hour_threshold[0]))

compactable['compacted_hour_threshold']=pd.to_datetime(compactable.compacted_hour_threshold)
compactable['last_full_hour_threshold']=pd.to_datetime(compactable.last_full_hour_threshold)
from dateutil import rrule
from datetime import datetime, timedelta
partition_adding_ddl = "ALTER TABLE streaming_mart ADD "
for dt in rrule.rrule(rrule.HOURLY, dtstart=compactable.compacted_hour_threshold[0], until=(compactable.last_full_hour_threshold[0] - timedelta(hours=1))):
	min_date_hour_string = dt.strftime("%Y-%m-%d %H:00:00")
	max_date_hour_string = (dt + timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
	min_date_string = dt.strftime("%Y-%m-%d")
	min_hour_string = dt.strftime("%H")
	sqlClient.execute_sql(
		"SELECT * FROM json_feed_view \
		 WHERE message_timestamp >= timestamp('{min_date_hour_string}') AND message_timestamp < timestamp('{max_date_hour_string}') \
		 INTO cos://us-south/my-data-lake-cos-bucket/landing_mart/message_date={min_date_string}/message_hour={min_hour_string} JOBPREFIX NONE STORED AS PARQUET \
		 ".format(min_date_hour_string=min_date_hour_string, max_date_hour_string=max_date_hour_string,
			   min_date_string=min_date_string, min_hour_string=min_hour_string))
	print("ETL of data between {} and {} (UTC) finished successfully.".format(min_date_hour_string, max_date_hour_string))
	partition_adding_ddl += "PARTITION (message_date ='{min_date_string}', message_hour='{min_hour_string}') \
				    LOCATION cos://us-south/my-data-lake-cos-bucket/landing_mart/message_date={min_date_string}/message_hour={min_hour_string} \
				   ".format(min_date_string=min_date_string, min_hour_string=min_hour_string)

if compactable.compactable_hours[0] > 0:
	sqlClient.execute_sql(partition_adding_ddl)
	print("Added {} new partitions to table streaming_mart.".format(compactable.compactable_hours[0]))

new_threshold = compactable.last_full_hour_threshold[0].strftime("%Y-%m-%d %H:00:00")
sqlClient.execute_sql(
	"CREATE OR REPLACE VIEW streaming_mart_realtime AS \
	 SELECT * FROM json_feed_view WHERE message_timestamp >= timestamp('{new_threshold}') \
	 UNION ALL \
	 SELECT device_id, message_timestamp, device_setup_time, payload_tz, row_number, longitude, latitude, pressure, temperature, humidity \
	 FROM streaming_mart \
	".format(new_threshold=new_threshold))

print("Updated view streaming_mart_realtime with threshold between landing and mart zone at message_timestamp={} (UTC).".format(new_threshold))

deletable_objects=sqlClient.run_sql("select * from deletable_landing_objects")
if deletable_objects is not None:
	for deletable_object in deletable_objects.input_object_path:
		df=sqlClient.delete_objects("cos://us-south/my-data-lake-cos-bucket/" + deletable_object)
	print("Successfully deleted {} compacted objects from landing zone.".format(deletable_objects.shape[0]))
else:
	print("No objects to delete from landing zone.")


