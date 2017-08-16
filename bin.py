from argparse import ArgumentParser

from model import Table
from config import *


def handle(pars):
    environments = {
        'dev': DB_DEV,
        'test': DB_TEST,
        'conf': DB_CONFIG,
    }

    op = pars.operate
    env = environments[pars.environment]
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
                        choices=['write', 'change_field_name', 'change_table_name', 'add_field', 'del_field'],
                        help='写入/修改字段/修改表名/添加字段')
    parser.add_argument('-e', '--environment', default='conf', help='数据库环境')
    parser.add_argument('-t', '--table', default='', help='要操作的表名')

    args = parser.parse_args()
    handle(args)
