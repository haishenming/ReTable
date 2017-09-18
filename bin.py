#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on 2017年8月14日

@author: xiangyufan@juwang.cn

修改数据表字段信息
'''

import os
import json
from argparse import ArgumentParser

import pymysql
from pymysql.err import ProgrammingError

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
TABLE_INFO_FILE = os.path.join(BASE_DIR, "table_info.json")


class DatabaseConf:
    """数据库配置类
    """

    xiang = {
        'host': '',  # 数据库地址
        'port': 3306,  # 数据库 端口
        'user': 'root',  # 用户名
        'password': '',  # 密码
    }

    # 开发数据库
    dev = {

    }

    # 测试数据库
    test = {

    }

    # 正式环境数据库
    publish = {

    }


class Table:
    def __init__(self, host, user, password, tables_name,
                 database='jc_backstage', port=3306, charset='utf8'):
        self.host = host  # 数据库地址
        self.port = port  # 数据库端口
        self.user = user  # 数据库用户名
        self.password = password  # 数据库密码
        self.database = database  # 数据库名
        if tables_name != [""]:  # 表名
            self.tables = tables_name
        else:
            tables_info = self.read_table_fields()
            self.tables = list(tables_info.keys())
        self.charset = charset  # 字符集
        self.conn = pymysql.connect(  # 数据库连接
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset
        )
        self.cursor = self.conn.cursor(
            cursor=pymysql.cursors.DictCursor)  # 数据库游标
        self.table_info = {}  # 要修改的数据配置
        self.error_msg = {  # 错误信息提示
            '0': "执行完毕！{}",
            '100': "sql语句执行错误：{}",
            '200': "缺少关键数据: {}",
            '300': "执行错误，{}"
        }

    @property
    def old_table_info_dict(self):
        """现有的表字段信息
        """
        tables_info_dict = {}
        for table in self.tables:
            table_info_dict = {
                "table_name": table,
                "table_field": {}
            }
            table_info = self.get_table_fields(table)
            field_list = ["Field", "Default", "Comment", "Type", "Null"]
            for field in table_info:
                needs_field = {}
                for k, v in field.items():
                    if k in field_list:
                        needs_field[k] = v
                table_info_dict["table_field"][field["Field"]] = needs_field
            tables_info_dict[table] = table_info_dict
        return tables_info_dict

    def get_table_fields(self, table):
        """获取表的字段信息
        """
        self.cursor.execute("show full columns from {};".format(table))
        table_fields = self.cursor.fetchall()
        self.conn.commit()
        return table_fields

    # def clean_file(self):
    #     """每次执行完操作后，清空文件中的json数据  ----该方法暂未使用
    #     """
    #     with open(TABLE_INFO_FILE, 'w', encoding="utf-8") as f:
    #         f.write(json.dumps({}, ensure_ascii=False, indent=4))

    def write_table_fields(self, _):
        """将表字段信息写入文件
        """
        try:
            with open(TABLE_INFO_FILE, 'w', encoding="utf-8") as f:
                f.write(
                    json.dumps(self.old_table_info_dict, ensure_ascii=False,
                               indent=4))
            return self.error_msg['0'].format("字段信息已写入文件")
        except Exception as e:
            return e

    def read_table_fields(self):
        """读取字段信息文件
        """
        with open(TABLE_INFO_FILE, 'r', encoding="utf-8") as f:
            table_info = json.load(f)
        if table_info:
            return table_info
        else:
            return self.error_msg['300'].format("每次执行操作前必须先执行write")

    def get_field(self, table_name, field_name):
        """获取表中的字段详细信息, 并返回该字段信息, 若不存在则返回None
        """
        for field, v in \
                self.old_table_info_dict[table_name]["table_field"].items():
            if field == field_name:
                return v
        return None

    def change_field_name(self, table_info, table_name):
        """修改字段名，并返回修改结果
        """
        if not isinstance(table_info, dict):
            return table_info
        field_names_dict = table_info['table_field']
        miss_fields_name_list = []
        err_field_name_list = []
        success_field_name = []
        for old_field_name, new_field_info in field_names_dict.items():
            field_info = self.get_field(table_name, old_field_name)
            if field_info == new_field_info:
                # 检查是否有修改
                continue
            if field_info:
                # 检查字段是否存在
                field_info.update(new_field_info)
                try:
                    if field_info["Null"] == "NO":
                        not_null = ' NOT NULL'
                    else:
                        not_null = None
                    sql = "ALTER TABLE `{table_name}` CHANGE `{old_name}`\
                            `{new_name}` {type}" \
                        .format(table_name=table_name,
                                old_name=old_field_name,
                                new_name=field_info['Field'],
                                type=field_info["Type"], )
                    if not_null:
                        sql += not_null
                    if field_info["Default"]:
                        sql += " DEFAULT " + r"'" + field_info[
                            "Default"] + r"'"
                    # else:
                    #     if field_info["Type"].startswith("int"):
                    #         sql += " DEFAULT '0' "
                    #     if field_info["Type"].startswith("varchar"):
                    #         sql += " DEFAULT \"\" "
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
        return self.error_msg['0'].format(
            "{}\n以下字段已修改:{},\n以下字段修改失败:{},\
            \n以下字段未找到:{}。\n".format(self.database + "." + table_name,
                                    success_field_name,
                                    err_field_name_list,
                                    miss_fields_name_list))

    def change_table_name(self, table_info, table_name):
        """修改表名
        """
        if not isinstance(table_info, dict):
            return table_info
        new_table_name = table_info['table_name']
        if new_table_name:
            try:
                # "ALTER TABLE now_table_name CHANGE olf_name new_
                # name VARCHAR(40) NOT NULL DEFAULT '' COMMENT '名称';"
                self.cursor.execute(
                    "ALTER TABLE `{}` RENAME TO `{}`;".format(table_name,
                                                              new_table_name))
                self.conn.commit()
            except ProgrammingError as e:
                # 如果sql语句执行出错，则返回错误信息
                return self.error_msg['100'].format(e)
        else:
            return self.error_msg['100'].format("未识别到新的表名")
        return self.error_msg['0'].format("修改表名成功\n{} ----> {}"
                                          .format(table_name, new_table_name))

    def add_field(self, table_info, table_name):
        """新增字段
        """
        if not isinstance(table_info, dict):
            return table_info
        err_field_name_list = []
        success_field_name = []
        for field_name, field_info in table_info['table_field'].items():
            field_name_is_new = True
            for old_field_name in \
                    self.old_table_info_dict[table_name]["table_field"].keys():
                if old_field_name == field_name:
                    field_name_is_new = False
            if field_name_is_new:
                try:
                    if field_info.get("Null") == "NO":
                        not_null = ' NOT NULL'
                    else:
                        not_null = None
                    sql = "ALTER TABLE `{table_name}` ADD \
                        `{new_name}` {type}".format(table_name=table_name,
                                                    new_name=field_name,
                                                    type=field_info["Type"], )
                    if not_null:
                        sql += not_null
                    if field_info.get("Default"):
                        sql += " DEFAULT " + r"'" + field_info[
                            "Default"] + r"'"
                    else:
                        if field_info["Type"].startswith("int"):
                            sql += " DEFAULT '0' "
                        if field_info["Type"].startswith("varchar"):
                            sql += " DEFAULT \"\" "
                    if field_info.get("Comment"):
                        sql += " COMMENT " + "'" + field_info["Comment"] + "'"
                    if not (field_info.get("Field") or field_info.get("Type")):
                        return self.error_msg["300"].format("Field和Type必填。")
                    self.cursor.execute(sql)
                    self.conn.commit()
                    success_field_name.append(field_name)
                except ProgrammingError as e:
                    # 如果sql语句执行出错，则返回错误信息
                    err_field_name_list.append((field_name, e))
        return self.error_msg['0'].format("{}\n新增以下字段成功:{},\
                                            \n以下字段操作失败:{}.\n"
                                          .format(table_name,
                                                  success_field_name,
                                                  err_field_name_list))

    def del_field(self, table_info, table_name):
        """删除字段
        """
        if not isinstance(table_info, dict):
            return table_info
        miss_fields_name_list = []
        err_field_name_list = []
        success_field_name = []
        for field_name, field_info in table_info['table_field'].items():
            for k, v in field_info.items():
                if k == "Del" and v:
                    try:
                        self.cursor.execute(
                            "ALTER TABLE `{}` DROP `{}`;".format(table_name,
                                                                 field_name))
                        self.conn.commit()
                        success_field_name.append(field_name)
                    except ProgrammingError as e:
                        # 如果sql语句执行出错，则返回错误信息
                        err_field_name_list.append((field_name, e))
        return self.error_msg['0'].format(
            "{}\n删除以下字段:{},\n以下字段删除失败:{},\n以下字段未找到:{}。\n".
                format(table_name, success_field_name, err_field_name_list,
                       miss_fields_name_list))

    def batch_op(self, op):
        tables_info = self.read_table_fields()
        if hasattr(self, op):
            func = getattr(self, op)
        else:
            return "没有该操作"
        ret_info = ""
        for table_name, table_info in tables_info.items():
            ret = func(table_info, table_name)
            ret_info += "\n" + ret
        return ret_info


def handle(pars):
    # 获取配置文件数据
    envs = DatabaseConf()
    if hasattr(envs, pars.environment):
        env = getattr(envs, pars.environment)
    else:
        print("环境未配置！")
        # 程序退出
        raise SystemExit

    op = pars.operate
    tables_name = pars.tables.split(",")
    env.update({"tables_name": tables_name})
    table = Table(**env)

    ops = {
        'write': table.write_table_fields,
        'change_field_name': table.batch_op,
        'change_table_name': table.batch_op,
        'add_field': table.batch_op,
        'del_field': table.batch_op,
    }

    ret = ops[op](op)
    print(ret)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-o', '--operate',
                        choices=['write', 'change_field_name',
                                 'change_table_name', 'add_field',
                                 'del_field'],
                        help='写入/修改字段/修改表名/添加字段/删除字段\n')
    parser.add_argument('-e', '--environment',
                        default='xiang',
                        help='数据库环境')
    parser.add_argument('-t', '--tables', default='', help='要操作的表名')

    pars = parser.parse_args()
    handle(pars=pars)
