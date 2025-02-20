from pyhive import hive
import mysql.connector

def select_from_hive(data, hive_host, hive_db, hive_table, username):
    # 使用用户名连接 Hive（不需要密码）
    conn = hive.Connection(
        host=hive_host,
        port=10000,
        username=username,  # 用户名
        auth='NONE'  # 无认证
    )
    cursor = conn.cursor()

    # 切换到指定数据库
    cursor.execute(f"USE {hive_db}")

    # 创建查询语句
    select_query = f"""
    SELECT `type`, `id`, SUM(traffic) AS total_traffic
    FROM {hive_table}
    WHERE `type` IN ('video', 'article')
    GROUP BY `type`, `id`
    ORDER BY total_traffic DESC
    LIMIT 10
    """
    
    # 执行查询语句
    cursor.execute(select_query)
    
    # 获取所有结果并保存到data中
    data = cursor.fetchall()

    # 关闭连接
    cursor.close()
    conn.close()

    # 返回查询结果
    return data


hive_host = '192.168.88.151'
hive_db = 'city'
hive_table = 'data2'
username = 'root'

# 保存查询结果
# data = select_from_hive([], hive_host, hive_db, hive_table, username)

# 打印查询结果
# print(data)

# try:
#     print("正在连接到Hive...")
#     hive_conn = hive.connect(host='192.168.88.151', port=10000, username='root')
#     hive_cursor = hive_conn.cursor()
#     print("Hive连接成功。")
# except Exception as e:
#     print(f"连接Hive时发生错误: {e}")
#     exit(1)


# try:
#     print("正在选择Hive数据库...")
#     hive_cursor.execute("USE city")  # 这里去掉了分号
#     print("Hive数据库选择成功。")
# except Exception as e:
#     print(f"选择Hive数据库时发生错误: {e}")
#     hive_cursor.close()
#     hive_conn.close()
#     exit(1)

# query = """
# SELECT `type`, `id`, SUM(traffic) AS total_traffic
# FROM data2
# WHERE `type` IN ('video', 'article')
# GROUP BY `type`, `id`
# ORDER BY total_traffic DESC
# LIMIT 10
# """
# try:
#     print("正在执行Hive查询...")
#     hive_cursor.execute(query)
#     result = hive_cursor.fetchall()
#     print(f"查询成功，返回了 {len(result)} 条结果。")
# except Exception as e:
#     print(f"执行Hive查询时发生错误: {e}")
#     hive_cursor.close()
#     hive_conn.close()
#     exit(1)


# try:
#     print("正在连接到MySQL...")
mysql_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='lian0000',
        database='city'
    )
mysql_cursor = mysql_conn.cursor()
#     print("MySQL连接成功。")
# except Exception as e:
#     print(f"连接MySQL时发生错误: {e}")
#     hive_cursor.close()
#     hive_conn.close()
#     exit(1)

# 创建数据表
# create_table_query = """
# CREATE TABLE IF NOT EXISTS first (
#     `type` VARCHAR(255),
#     `id` VARCHAR(255),
#     total_traffic BIGINT
# );
# """
# try:
#     print("正在创建MySQL表...")

# 运行创建表的语句
# mysql_cursor.execute(create_table_query)



#     print("MySQL表创建成功。")
# except Exception as e:
#     print(f"创建MySQL表时发生错误: {e}")

#     hive_cursor.close()
#     hive_conn.close()
#     exit(1)


# try:
#     print(f"正在将 {len(result)} 条数据插入MySQL...")
insert_query = """
INSERT INTO first (`type`, `id`, total_traffic)
VALUES (%s, %s, %s)
"""
data=[('video', '10506', 3145836), ('video', '5609', 1048643), ('video', '8175', 1048630), ('video', '11724', 1048576), ('video', '9478', 1048576), ('video', '11938', 1048576), ('video', '13109', 1048576), ('video', '2030', 552862), ('video', '7557', 524396), ('video', '13181', 524288)]
#     # 使用批量插入
mysql_cursor.executemany(insert_query, data)

mysql_conn.commit()  # 确保数据已提交
print("数据插入成功。")
#     mysql_conn.commit()
#     print("数据插入成功。")
# except Exception as e:
#     print(f"插入数据到MySQL时发生错误: {e}")
#     mysql_conn.rollback()  # 如果插入失败，回滚事务

mysql_cursor.close()
mysql_conn.close()
# finally:

#     mysql_cursor.close()
#     mysql_conn.close()
#     hive_cursor.close()
#     hive_conn.close()
#     print("所有连接已关闭。")
