import pymysql

class Mysql:
    instance = None
    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = object.__new__(cls)
        return cls.instance
    
    def __init__(self, db_config):
        if not getattr(self, 'connection', 0) or getattr(self, 'cursor', 0):
            self.close()
            # 打开数据库连接
            self.connection = pymysql.connect(**db_config)
            # 使用cursor()方法获取操作游标 
            self.cursor = self.connection.cursor()
    
    def close(self):
        if getattr(self, 'cursor', 0):
            self.cursor.close()
            self.cursor = None
        if getattr(self, 'connection', 0):
            self.connection.close()
            self.connection = None
        
    def __del__(self):
        self.close()
        
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        # print(exc_info)
        self.close()
        
    def create_table(self, table_name, col_dict, del_exist=False):
        """
        col_dict 格式为字典，键为字段名，值为字段的约束，
            例如：{'id': 'int auto_increment primary key', 'name': 'char(20)', 'age': 'int unsigned'}
        del_exists 如果要创建的表已存在，进行的操作。
            为真则删除原表，强制创建新表；为假则保留原表，放弃创建新表
        """
        fields = ','.join([' '.join(item) for item in col_dict.items()])
        if del_exist:
            self.cursor.execute(f'drop table if exists {table_name};')
            create_sql = f'create table {table_name} ({fields});'
        else:
            create_sql = f'create table if not exists {table_name} ({fields});'
        self.cursor.execute(create_sql)
    
    def drop_table(self, table_name):
        drop_sql = f'drop table if exists {table_name}'
        self.cursor.execute(drop_sql)
        
    def exist_table(self, table_name):
        """判断一个表是否存在
        Args:
            table_name (str): 表格名
        """
        exist_sql = f'SELECT table_name FROM information_schema.TABLES WHERE table_name ="{table_name}";'
        self.cursor.execute(exist_sql)
        return bool(self.cursor.rowcount)
    
    def get_value_str_list(self, values):
        value_str_list = []
        for value in values:
            if isinstance(value, str):
                value_str_list.append(f'"{value}"')
            else:
                value_str_list.append(str(value))
        return value_str_list
        
    def insert(self, table_name, columns, values):
        """向指定表中插入数据

        Args:
            table_name (str): 表名
            columns (iterable): 需要插入数据的列
            values (iterable): 需要插入的数据的值，如果是单行数据，以列表传入；多行数据以列表套列表形式传入
        """
        multi_flag = True
        for item in values:
            if not isinstance(item, (list, tuple)):
                multi_flag = False
                break
        else:
            if len(max(values, key=lambda x: len(x))) != len(min(values, key=lambda x: len(x))):
                multi_flag = False
        if multi_flag:
            value_sql_list = []
            for value in values:
                value_str_list = self.get_value_str_list(value)
                value_sql_list.append(f'({",".join(value_str_list)})')
            value_sql = ','.join(value_sql_list)
        else:
            value_str_list = self.get_value_str_list(values)
            value_sql = f'({",".join(value_str_list)})'
        insert_sql = f'insert into {table_name} ({",".join(columns)}) values {value_sql}'
        self.cursor.execute(insert_sql)
        self.connection.commit()
    
    def delete(self, table_name, conditions, relation='and'):
        """删除数据行

        Args:
            table_name (str): 表名
            conditions (dict): 删除条件
            relation (str): 删除条件之间的关系，and 或者 or
        """
        relation = relation.lower()
        if relation not in ('and', 'or'):
            # TODO 这里其实可以抛出一个异常，将来或许可以整理一个异常类
            return
        relation = f' {relation} '
        condition_sql_list = []
        for field, value in conditions.items():
            if isinstance(value, str):
                condition_sql_list.append(f'{field}="{value}"')
            elif isinstance(value, (list, tuple)):
                value_str_list = self.get_value_str_list(value)
                condition_sql_list.append(f'{field} in ({",".join(value_str_list)})')
            else:
                condition_sql_list.append(f'{field}={value}')
        delete_sql = f'delete from {table_name} where {relation.join(condition_sql_list)}'
        print(delete_sql)
        self.cursor.execute(delete_sql)
        self.connection.commit()
        
        
if __name__ == "__main__":
    db_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '',
        'database': 'exercise',
        'port': 3306
    }
    # mysql = Mysql(db_config)
    col_dict = {'id': 'int primary key auto_increment', 'name': 'char(20)', 'age': 'int unsigned'}
    columns = ['name', 'age']
    # values = ['shuo', 12]
    values = [['alice', 11], ['bob', 33]]
    conditions = {'name': ['alice', 'bob'], 'age': 11}
    # mysql.create_table('test', col_dict, del_exist=True)
    # mysql.drop_table('test')
    with Mysql(db_config) as mysql:
        # mysql.create_table('test', col_dict, del_exist=True)
        # mysql.drop_table('test')
        # print(mysql.exist_table('test'))
        # mysql.insert('test', columns, values)
        mysql.delete('test', conditions)
        pass
    mysql.close()