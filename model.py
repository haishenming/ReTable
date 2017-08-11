"""
修改mysql表
"""

import json

import pymysql
from pymysql.err import ProgrammingError
from config import DB_CONFIG


class Table:
    def __init__(self, host, port, user, password, database, table_name, charset='utf8'):
        self.host = host  # 数据库地址
        self.port = port  # 数据库端口
        self.user = user  # 数据库用户名
        self.password = password  # 密码
        self.database = database  # 数据库名
        self.table = table_name  # 表名
        self.charset = charset  # 字符集
        self.conn = pymysql.connect(  # 数据库连接
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset
        )
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)  # 数据库游标
        self.table_info = {}  # 要修改的数据配置
        self.error_msg = {  # 错误信息提示
            '0': "执行完毕！{}",
            '100': "sql语句执行错误：{}",
            '200': "缺少关键数据: {}",
            '300': "执行错误，{}"
        }

    def get_table_fields(self):
        """获取表的字段信息
        """
        self.cursor.execute("show full columns from {};".format(self.table))
        table_fields = self.cursor.fetchall()
        self.conn.commit()

        return table_fields

    @property
    def old_table_info_dict(self):
        """旧的表字段信息
        """
        table_info_dict = {
            "table_name": self.table,
            "table_field": {}
        }
        table_info = self.get_table_fields()
        field_list = ["Field", "Default", "Comment", "Type", "Null"]
        for field in table_info:
            needs_field = {}
            for k, v in field.items():
                if k in field_list:
                    needs_field[k] = v
            table_info_dict["table_field"][field["Field"]] = needs_field
        return table_info_dict

    def clean_file(self):
        """每次执行完操作后，清空文件中的json字典
        """
        with open("table_info.txt", 'w', encoding="utf-8") as f:
            f.write(json.dumps({}, ensure_ascii=False, indent=4))

    def write_table_fields(self):
        """将表字段信息写入文件
        """
        try:
            with open("table_info.txt", 'w', encoding="utf-8") as f:
                f.write(json.dumps(self.old_table_info_dict, ensure_ascii=False, indent=4))
            return self.error_msg['0'].format("表字段信息已写入文件")
        except Exception as e:
            return e

    def read_table_fields(self):
        """读取字段信息文件
        """
        with open("table_info.txt", 'r', encoding="utf-8") as f:
            table_info = json.load(f)
        if table_info:
            return table_info
        else:
            return self.error_msg['300'].format("每次执行操作前必须先执行write")

    def get_field(self, field_name):
        """获取表中的字段详细信息, 并返回该字段信息, 若不存在则返回None
        """
        for field, v in self.old_table_info_dict["table_field"].items():
            if field == field_name:
                return v

        return None

    def change_field_name(self):
        """修改字段名，并返回修改结果
        """
        table_info = self.read_table_fields()
        if not isinstance(table_info, dict):
            return table_info
        field_names_dict = table_info['table_field']
        miss_fields_name_list = []
        err_field_name_list = []
        success_field_name = []
        for old_field_name, new_field_info in field_names_dict.items():
            old_field_name = old_field_name
            new_field_info = new_field_info
            field_info = self.get_field(old_field_name)
            if field_info:
                field_info.update(new_field_info)
                # 检查字段是否存在
                try:
                    if field_info["Null"] == "NO":
                        not_null = ' NOT NULL'
                    else:
                        not_null = None
                    sql = "ALTER TABLE `{table_name}` CHANGE `{old_name}` `{new_name}` {type}". \
                        format(table_name=self.table,
                               old_name=old_field_name,
                               new_name=field_info['Field'],
                               type=field_info["Type"], )
                    if not_null:
                        sql += not_null
                    if field_info["Default"]:
                        sql += " DEFAULT " + r"'" + field_info["Default"] + r"'"
                    else:
                        if field_info["Type"].startswith("int"):
                            sql += " DEFAULT '0' "
                        if field_info["Type"].startswith("varchar"):
                            sql += " DEFAULT \"\" "
                    if field_info["Comment"]:
                        sql += " COMMENT " + "'" + field_info["Comment"] + "'"
                    self.cursor.execute(sql)
                    self.conn.commit()
                    success_field_name.append(old_field_name)
                except ProgrammingError as e:
                    # 如果sql语句执行出错，则返回错误信息
                    err_field_name_list.append((old_field_name, e))
            else:
                miss_fields_name_list.append(old_field_name)
        self.clean_file()
        return self.error_msg['0'].format("\n以下字段已修改:{},\n以下字段修改失败:{},\n以下字段未找到:{}。\n"
                                          .format(success_field_name, err_field_name_list, miss_fields_name_list))

    def change_table_name(self):
        """修改表名
        """
        table_info = self.read_table_fields()
        if not isinstance(table_info, dict):
            return table_info
        new_table_name = table_info['table_name']
        if new_table_name:
            try:
                # "ALTER TABLE now_table_name CHANGE olf_name new_name VARCHAR(40) NOT NULL DEFAULT '' COMMENT '名称';"
                ret = self.cursor.execute("ALTER TABLE `{}` RENAME TO `{}`;".format(self.table, new_table_name))
                self.conn.commit()
            except ProgrammingError as e:
                # 如果sql语句执行出错，则返回错误信息
                return self.error_msg['100'].format(e)
        else:
            return self.error_msg['100'].format("未识别到新的表名")
        self.clean_file()
        return self.error_msg['0'].format("修改表名成功，请同时修改掉配置文件中的表名,\n{} ----> {}"
                                          .format(self.table, new_table_name))

    def add_field(self):
        """新增字段
        """
        table_info = self.read_table_fields()
        if not isinstance(table_info, dict):
            return table_info
        err_field_name_list = []
        success_field_name = []
        for field_name, field_info in table_info['table_field'].items():
            field_name_is_new = True
            for old_field_name in self.old_table_info_dict["table_field"].keys():
                if old_field_name == field_name:
                    field_name_is_new = False
            if field_name_is_new:
                try:
                    if field_info["Null"] == "NO":
                        not_null = ' NOT NULL'
                    else:
                        not_null = None
                    sql = "ALTER TABLE `{table_name}` ADD `{new_name}` {type}". \
                        format(table_name=self.table,
                               new_name=field_name,
                               type=field_info["Type"], )
                    if not_null:
                        sql += not_null
                    if field_info["Default"]:
                        sql += " DEFAULT " + r"'" + field_info["Default"] + r"'"
                    else:
                        if field_info["Type"].startswith("int"):
                            sql += " DEFAULT '0' "
                        if field_info["Type"].startswith("varchar"):
                            sql += " DEFAULT \"\" "
                    if field_info["Comment"]:
                        sql += " COMMENT " + "'" + field_info["Comment"] + "'"
                    self.cursor.execute(sql)
                    self.conn.commit()
                    success_field_name.append(field_name)
                except ProgrammingError as e:
                    # 如果sql语句执行出错，则返回错误信息
                    err_field_name_list.append((field_name, e))
        self.clean_file()
        return self.error_msg['0'].format("新增以下字段成功:{},\n以下字段操作失败:{}.\n"
                                          .format(success_field_name, err_field_name_list))

    def del_field(self):
        """删除字段
        """
        pass
        table_info = self.read_table_fields()
        if not isinstance(table_info, dict):
            return table_info
        miss_fields_name_list = []
        err_field_name_list = []
        success_field_name = []
        for field_name, field_info in table_info['table_field'].items():
            for k, v in field_info.items():
                if k == "del" and v:
                    try:
                        self.cursor.execute("ALTER TABLE `{}` DROP `{}`;".format(self.table, field_name))
                        self.conn.commit()
                        success_field_name.append(field_name)
                    except ProgrammingError as e:
                        # 如果sql语句执行出错，则返回错误信息
                        err_field_name_list.append((field_name, e))
            self.clean_file()
        return self.error_msg['0'].format("\n删除以下字段:{},\n以下字段删除失败:{},\n以下字段未找到:{}。\n"
                                        .format(success_field_name, err_field_name_list, miss_fields_name_list))



            # ALTER TABLE teaching_train DROP new_field;