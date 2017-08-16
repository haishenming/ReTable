import os

# 数据库配置
DB_CONFIG = {
    'host': '101.201.68.4',  # 数据库地址
    'port': 3306,  # 数据库端口
    'user': 'root',  # 用户名
    'password': '',  # 密码
    'database': '',  # 数据库
}

# 开发数据
DB_DEV = {

}

# 测试数据库
DB_TEST = {

}


BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
TABLE_INFO_FILE = os.path.join(BASE_DIR, "table_info.json")
