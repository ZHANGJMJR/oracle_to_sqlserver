import cx_Oracle
import pyodbc
import pandas as pd
from datetime import datetime
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

# ---------------------- 日志配置 ----------------------
def setup_logger(log_dir=None, log_level=logging.INFO, log_prefix="migration"):
    """
    设置日志记录器

    参数:
        log_dir: 日志文件存储目录，如果为None则使用当前工作目录
        log_level: 日志级别，默认为INFO
        log_prefix: 日志文件名前缀，默认为"migration"
    """
    # 如果未指定目录，使用当前工作目录
    if log_dir is None:
        log_dir = os.getcwd()

    # 确保目录存在
    os.makedirs(log_dir, exist_ok=True)

    # 构建日志文件名
    log_file = os.path.join(log_dir, f"{log_prefix}_log_{datetime.now().strftime('%Y%m%d')}.log")

    # 配置日志记录器
    logger = logging.getLogger('migration_logger')
    logger.setLevel(log_level)

    # 创建文件处理器
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # 创建格式化器并添加到处理器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器到日志记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
# ---------------------- 日志配置 ---结束-------------------

# # 设置全局日志记录器
# logger = setup_logger()
def migrate_data():

    # Oracle数据库连接配置
    oracle_user = 'XYJT2022'
    oracle_password = 'XYJT2022'
    oracle_dsn = cx_Oracle.makedsn('192.168.100.6', 1521, service_name='xyjteas')

    # SQL Server连接配置
    sql_server_driver = '{ODBC Driver 17 for SQL Server}' #'{SQL Server}'  # 或者使用 '{ODBC Driver 17 for SQL Server}'
    sql_server_server = '127.0.0.1'
    sql_server_database = 'powerautomate'
    sql_server_username = 'sa'
    sql_server_password = 'sa'

    try:
        logger.info("开始连接Oracle数据库...")
        # 连接到Oracle数据库
        oracle_conn = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
        # print("成功连接到Oracle数据库")
        logger.info("成功连接到Oracle数据库")

        logger.info("开始连接SQL Server...")
        # 连接到SQL Server
        sql_server_conn_str = f'DRIVER={sql_server_driver};SERVER={sql_server_server};DATABASE={sql_server_database};UID={sql_server_username};PWD={sql_server_password};Encrypt=no'
        sql_server_conn = pyodbc.connect(sql_server_conn_str)
        sql_server_cursor = sql_server_conn.cursor()
        # print("成功连接到SQL Server")
        logger.info("成功连接到SQL Server")
        # 要迁移的表列表
        tables_to_migrate = ['T_PM_User', 'T_ORG_BASEUNIT','T_BD_Person']

        for table in tables_to_migrate:
            logger.info(f"\n开始迁移表 {table}...")
            # print(f"\n开始迁移表 {table}...")

            logger.info(f"清空SQL Server中的表 {table}...")
            # 清空SQL Server中的目标表
            sql_server_cursor.execute(f"TRUNCATE TABLE {table}")
            sql_server_conn.commit()
            # print(f"已清空SQL Server中的表 {table}")
            logger.info(f"已清空SQL Server中的表 {table}")

            logger.info(f"从Oracle查询表 {table} 数据...")
            # 从Oracle获取数据
            oracle_cursor = oracle_conn.cursor()
            if table == 'T_BD_Person':
                oracle_cursor.execute(f"select FID, FNUMBER,FNAME_L2,FCELL  from {table}")
            else:
                oracle_cursor.execute(f"SELECT FID,FNAME_L2,FNUMBER FROM {table}")

            columns = [desc[0] for desc in oracle_cursor.description]
            data = oracle_cursor.fetchall()
            oracle_cursor.close()

            if not data:
                logger.info(f"Oracle中表 {table} 没有数据，跳过")
                # print(f"Oracle中表 {table} 没有数据，跳过")
                continue
            logger.info(f"从Oracle获取了 {len(data)} 条记录")
            # print(f"从Oracle获取了 {len(data)} 条记录")

            # 将数据转换为DataFrame以便处理
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"已将数据转换为DataFrame，形状: {df.shape}")


            # 构建插入语句
            placeholders = ', '.join(['?' for _ in range(len(columns))])
            insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

            # 批量插入数据
            batch_size = 1000
            logger.info(f"开始批量插入数据，每批 {batch_size} 条记录")
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size].values.tolist()
                # 处理datetime对象，确保它们是pyodbc可以处理的格式
                for row in batch:
                    for j, value in enumerate(row):
                        if isinstance(value, datetime):
                            row[j] = value.strftime('%Y-%m-%d %H:%M:%S')

                sql_server_cursor.executemany(insert_query, batch)
                sql_server_conn.commit()
                logger.info(f"已插入 {min(i + batch_size, len(df))}/{len(df)} 条记录")
                # print(f"已插入 {min(i + batch_size, len(df))}/{len(df)} 条记录")

            logger.info(f"表 {table} 迁移完成")
            # print(f"表 {table} 迁移完成")

        logger.info("\n所有表迁移完成！")
        # print("\n所有表迁移完成！")

    except Exception as e:
        logger.error(f"发生错误: {str(e)}", exc_info=True)
        # print(f"发生错误: {str(e)}")
    finally:
        # 关闭连接
        if 'oracle_conn' in locals():
            oracle_conn.close()
            # print("Oracle连接已关闭")
            logger.info("Oracle连接已关闭")
        if 'sql_server_conn' in locals():
            sql_server_conn.close()
            logger.info("SQL Server连接已关闭")
            # print("SQL Server连接已关闭")


if __name__ == "__main__":
    log_dir = sys.argv[1] if len(sys.argv) > 1 else None
    logger = setup_logger(log_dir)
    migrate_data()
