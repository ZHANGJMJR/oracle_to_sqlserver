import cx_Oracle
import pyodbc
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

# ---------------------- 日志配置 ----------------------
def setup_logger(log_file_prefix="migration_log", log_level=logging.INFO, backup_count=30):
    """
    设置日志记录器，配置按日期分割的日志文件和控制台输出

    参数:
        log_file_prefix: 日志文件名前缀
        log_level: 日志级别，默认为INFO
        backup_count: 保留的历史日志文件数量
    """
    # 生成按日期分割的日志文件
    log_file = f"{log_file_prefix}_{datetime.now().strftime('%Y%m%d')}.log"

    # 创建logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # 确保没有重复的处理器
    if logger.handlers:
        logger.handlers.clear()

    # 日志格式：时间 - 级别 - 模块名 - 消息
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 文件处理器（按天分割日志）
    file_handler = TimedRotatingFileHandler(
        log_file,
        when='midnight',  # 每天午夜分割日志
        interval=1,
        backupCount=backup_count,  # 保留的历史日志数量
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # 添加处理器到logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger
# ---------------------- 日志配置 ---结束-------------------

# 设置全局日志记录器
logger = setup_logger()
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
        tables_to_migrate = ['T_PM_User', 'T_ORG_BASEUNIT']

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
    migrate_data()
