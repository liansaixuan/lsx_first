import pandas as pd
import requests
from pyhive import hive
from datetime import datetime

# 获取城市信息（根据 IP 地址）
def get_city_by_ip(ip):
    try:
        url = f'http://ip-api.com/json/{ip}?fields=city&lang=zh-CN'
        response = requests.get(url, timeout=50)
        data = response.json()
        # 如果返回没有城市信息或错误，返回'Unknown'
        return data.get('city', 'Unknown')
    except Exception as e:
        print(f"Error fetching city for IP {ip}: {e}")
        return 'Unknown'

# 数据清洗函数
# 数据清洗函数
def clean_data(file_path):
    rows = []
    
    # 读取 result.txt 文件中的数据
    with open(file_path, 'r') as file:
        raw_data = file.readlines()
        
        for line in raw_data:
            # 跳过空行
            if not line.strip():
                continue
            
            try:
                # 分割并处理每个字段
                parts = [part.strip() for part in line.split(',')]  # 使用strip()去除空格
                
                # 确保每行都有足够的部分
                if len(parts) < 5:
                    print(f"Skipping malformed line: {line.strip()}")
                    continue
                
                ip = parts[0].strip()
                time_str = parts[1].strip()
                day=parts[2].strip()
                traffic_str = parts[3].strip()  # 这里应该是字符串
                type_str = parts[4].strip()
                item_id = parts[5].strip()

                # 确保 traffic 是数字，如果不是，跳过该行
                if not traffic_str.isdigit():
                    print(f"Skipping line with invalid traffic: {line.strip()}")
                    continue
                traffic = int(traffic_str)  # 强制转换为整数

                # 提取时间
                time_obj = datetime.strptime(time_str, "%d/%b/%Y:%H:%M:%S +0800")
                date = time_obj.strftime('%Y-%m-%d %H:%M:%S')
                # day = str(time_obj.day)  # 使用day作为当天日期的唯一标识符
                
                # 获取城市
                city = get_city_by_ip(ip)
                
                # 确保 id 是字符串类型，避免丢失
                id_value = str(item_id)  # 强制转换为字符串

                # 构造清洗后的数据
                row = {
                    'ip': ip,
                    'city': city,
                    'time': date,
                    'day': day,
                    'traffic': traffic,  # 保证是整数
                    'type': type_str,
                    'id': id_value  # 确保id没有丢失
                }
                rows.append(row)
            except Exception as e:
                print(f"Error processing line: {line.strip()} | Error: {e}")
    
    return pd.DataFrame(rows)

# 将清洗后的数据插入到Hive
def insert_to_hive(data, hive_host, hive_db, hive_table, username):
    # 使用用户名连接 Hive（不需要密码）
    conn = hive.Connection(
        host=hive_host,
        port=10000,
        username=username,  # 用户名
        # password="",  # 密码为空
        auth='NONE'  # 无认证
    )
    cursor = conn.cursor()
    
    # 创建插入语句
    insert_query = f"INSERT INTO {hive_db}.{hive_table} (ip, `time`, `day`, traffic, `type`, id,city) VALUES "
    values = []
    
    for index, row in data.iterrows():
        # 使用 str(row['traffic']) 来确保 traffic 转换为字符串格式，bigint 在 Hive 中通常是数值类型，但插入时用字符串也可以。
        # ip_city = row['ip'] + ',' + row['city']
        values.append(f"('{row['ip']}', '{row['time']}', '{row['day']}', {row['traffic']}, '{row['type']}', '{row['id']}','{row['city']}')")
    
    insert_query += ", ".join(values)
    
    # 执行插入语句
    cursor.execute(insert_query)
    cursor.close()
    conn.close()


# 读取 result.txt 文件路径
file_path = r'D:\PythonStudy\2.19建民数据清洗\result.txt'

# Hive 连接信息
hive_host = "192.168.88.151"
hive_db = "city"
hive_table = "data3"
username = "root"  # 替换为你的用户名

# 清洗数据
cleaned_data = clean_data(file_path)

# 输出清洗后的数据
print(cleaned_data)

# 将清洗后的数据插入到 Hive
insert_to_hive(cleaned_data, hive_host, hive_db, hive_table, username)
