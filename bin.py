from argparse import ArgumentParser

from model import Table
from config import *


def handle(args):
    environments = {
        'dev': DB_DEV,
        'test': DB_TEST,
        'conf': DB_CONFIG
    }

    op = args.operate
    env = environments[args.environment]
    table_name = args.table
    env.update({"table_name": table_name})
    table = Table(**env)

    ops = {
        'write': table.write_table_fields,
        'change_field_name': table.change_field_name,
        'change_table_name': table.change_table_name,
        'add_field': table.add_field,
        'del_field': table.del_field
    }

    ret = ops[op]()
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
