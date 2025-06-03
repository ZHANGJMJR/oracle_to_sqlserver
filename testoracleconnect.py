# 测试Oracle连接
import cx_Oracle

try:
    oracle_user = 'XYJT2025'
    oracle_password = 'XYJT2025'
    oracle_dsn = cx_Oracle.makedsn('192.168.100.60', 1521, service_name='xyjt')

    conn = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
    print("Oracle连接成功!")
    conn.close()
except Exception as e:
    print(f"Oracle连接失败: {e}")

# 测试SQL Server连接
import pyodbc

try:
    sql_server_driver = '{ODBC Driver 17 for SQL Server}'
    sql_server_server = '127.0.0.1'
    sql_server_database = 'powerautomate'
    sql_server_username = 'sa'
    sql_server_password = 'sa'

    conn_str = f'DRIVER={sql_server_driver};SERVER={sql_server_server};DATABASE={sql_server_database};UID={sql_server_username};PWD={sql_server_password};Encrypt=no'
    conn = pyodbc.connect(conn_str)
    print("SQL Server连接成功!")
    conn.close()
except Exception as e:
    print(f"SQL Server连接失败: {e}")