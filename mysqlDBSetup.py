import sqlalchemy
import pymysql
import google
import base64
import os
from google.cloud import logging

def main(req):

	#set up logging 
	logging_client = logging.Client()
	log_name = "db_log"
	logger = logging_client.logger(log_name)
	print("Start")

	#set up MySql DB connection 
	connection_name = "caramel-element-338723:us-central1:cisc-520-db"
	db_name = "accident_data_db"
	db_user = "root"
	db_password = "12345"
	db_host = "35.232.242.203"
	db_port = "3306"
	driver_name = 'mysql+pymysql'
	query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
 
	db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
		host=db_host, 
		port=db_port,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )

	# execution
	print("start DB")
	with db.connect() as conn:
		print("start execution")
		res = conn.execute("CREATE TABLE IF NOT EXISTS MAIN(ACCIDENT_DATE DATE,ACCIDENT_TIME time,BOROUGH CHAR(20),ZIP_CODE CHAR(5),LATITUDE float,LONGITUDE float,LOCATION CHAR(100),ON_STREET_NAME CHAR(50),CROSS_STREET_NAME CHAR(50),OFF_STREET_NAME CHAR(50),NUMBER_OF_PERSONS_INJURED INT,NUMBER_OF_PERSONS_KILLED INT,NUMBER_OF_PEDESTRIANS_INJURED INT,NUMBER_OF_PEDESTRIANS_KILLED INT,NUMBER_OF_CYCLIST_INJURED INT,NUMBER_OF_CYCLIST_KILLED INT,NUMBER_OF_MOTORIST_INJURED INT,NUMBER_OF_MOTORIST_KILLED INT,CONTRIBUTING_FACTOR_VEHICLE_1 CHAR(100),CONTRIBUTING_FACTOR_VEHICLE_2 CHAR(100),CONTRIBUTING_FACTOR_VEHICLE_3 CHAR(100),CONTRIBUTING_FACTOR_VEHICLE_4 CHAR(100),CONTRIBUTING_FACTOR_VEHICLE_5 CHAR(100),COLLISION_ID CHAR(7),VEHICLE_TYPE_CODE_1 CHAR(50),VEHICLE_TYPE_CODE_2 CHAR(50),VEHICLE_TYPE_CODE_3 CHAR(50),VEHICLE_TYPE_CODE_4 CHAR(50),VEHICLE_TYPE_CODE_5 CHAR(50));")
		columns = res.keys()
		print ("columns: {}".format(columns))
		res = conn.execute("SELECT * FROM MAIN LIMIT 10;").fetchall()
		for row in res:
			print(row)
		print("end execution")

	return ("SUCCESS")
