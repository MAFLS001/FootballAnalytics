import mysql.connector
from mysql.connector import Error
# Connect
try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="root", database='centre_for_psychology',
                                         connection_timeout=9000, auth_plugin='mysql_native_password')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor(buffered=True)
        connection.autocommit = True
        global_interactive_timeout = 'SET GLOBAL connect_timeout=9000'
        cursor.execute("""ALTER TABLE appointment AUTO_INCREMENT = 1""")
except Error as e:
    print("Error while connecting to MySQL", e)