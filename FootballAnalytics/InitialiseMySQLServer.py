import mysql.connector
database_name = "u496378222_Football"
my_username = "u496378222_SMAFLIN"
password = "Billabong6583!"
host = "145.14.152.202"
server = "HardlyBaflin"

offline_database_name = "u496378222_Football"
offline_my_username = "root"
offline_password = "root"
offline_host = "localhost"
offline_server = "Offline HardlyBaflin "


# Establish connection
cnx = mysql.connector.connect(
    host=offline_host,
    user=offline_my_username,
    password=offline_password,
    database=offline_database_name,
    connect_timeout=600
)
# Check if connected and print a message
if cnx.is_connected():
    print("Connected to the " + offline_server + " MySQL server!")
# Create a cursor
cursor = cnx.cursor()


#cd "C:\Program Files\MySQL\MySQL Server 8.0\bin"
#mysqldump --host=145.14.152.202 --port=3306 --user=u496378222_SMAFLIN --password=Billabong6583! --protocol=tcp --skip-triggers --skip-column-statistics u496378222_Football > "C:\Users\SMafl\Documents\dumps\Dump20230705.sql"