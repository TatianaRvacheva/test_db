"""Проверка создания, изменения и удаления таблицы"""

import unittest
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

import psycopg2
from parameterized import parameterized, parameterized_class


@dataclass
class DBData:
    """Данные о БД"""
    db_name: str = 'test_db'
    user: str = 'test_user'
    password: str = 'Pass'
    host: str = 'localhost'
    port: str = '5432'
    table_name: str = 'People'
    table_params: ClassVar[list[tuple]] = [
        ('Index', 'SERIAL PRIMARY KEY'),
        ('Name', 'TEXT'),
        ('DataOfBirth', 'DATE')
    ]
    table_data: ClassVar[list[tuple]] = [
        ('Иван', '1990-01-01'),
        ('Пётр', '1988-08-31'),
        ('Екатерина', '1995-05-22')
    ]
    change_data = [
        ('Екатерина', '1995-06-22'),
        ('Пётр', '1985-08-31')
    ]
    neg_change_data = [
        ('Екатерина', 'NULL')
    ]


db = DBData()


def send_db_command(**kwargs):
    """Соединение с БД и выполнение запросов"""
    data = kwargs
    try:
        with psycopg2.connect(database=data['db_name'], user=data['user'], password=db.password, host=db.host, port=db.port) as conn:
            with conn.cursor() as cursor:
                for command in data['commands']:
                    cursor.execute(command)
                    if 'SELECT' in command:  # получение ответа на запрос SELECT
                        return cursor.fetchall()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def create_sql_cmd(cmd: str, data: list, method: str):
    """Формирование SQL-запроса"""
    for x in data:
        index = data.index(x)
        last_symbol = ';' if index == (len(data) - 1) else ','
        msg = f' {str(x)}{last_symbol}' if method == 'INSERT' else f' {x[0]} {x[1]}{last_symbol}'
        cmd += msg
    return cmd


# дописываются через запятую другие параметры стенда: название БД, имя пользователя, название таблицы
@parameterized_class(('db_name', 'user', 'table_name'), [(db.db_name, db.user, db.table_name)])
class TestDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Создание и заполнение данными тестируемой таблицы"""
        create_cmd = f'CREATE TABLE {cls.table_name} ('
        create_cmd = create_sql_cmd(cmd=create_cmd, data=db.table_params, method='CREATE').replace(';',');')

        print(f'Запрос на создание таблицы {cls.table_name} отправлен...')
        send_db_command(commands=[create_cmd], db_name=cls.db_name, user=cls.user)

    @classmethod
    def tearDownClass(cls):
        """Удаление тестируемой таблицы"""
        print(f'Тестовая таблица {cls.table_name} будет удалена')
        send_db_command(commands=[f'DROP TABLE {cls.table_name}'], db_name=cls.db_name, user=cls.user)

    def check_data(self, result, excpected_data=None, cup=True):
        if not excpected_data:
            excpected_data = db.table_data
        for item in result:
            index = result.index(item)
            check_data = item[1:]if cup else item   # пропускать проверку ПП Index в начальных данных
            for i in check_data:
                if isinstance(i, date):
                    i = i.strftime('%Y-%m-%d')
                self.assertIn(i, excpected_data[index], msg=f'ERROR: {i} not found in {excpected_data[index]}')
        print(f'Все данные {result} присутствуют')

    def test_add_data(self):
        insert_cmd = f"INSERT INTO {self.table_name} (Name, DataOfBirth) VALUES"
        insert_cmd = create_sql_cmd(cmd=insert_cmd, data=db.table_data, method='INSERT')
        select_cmd = f"SELECT * FROM {self.table_name};"

        result = send_db_command(commands=[insert_cmd, select_cmd], db_name=self.db_name, user=self.user)
        self.assertIsNotNone(result, msg='SELECT CMD: No data about table')
        self.check_data(result)

    @parameterized.expand(db.change_data)   # можно масштабировать и увеличить в коли-ве параметры для изменения
    def test_update_data(self, name, dateofbirth):
        update_cmd = f"UPDATE {self.table_name} SET DataOfBirth = '{dateofbirth}' WHERE Name = '{name}';"
        select_cmd = f"SELECT Name, DataOfBirth FROM {self.table_name} WHERE Name =  '{name}';"

        result = send_db_command(commands=[update_cmd, select_cmd], db_name=self.db_name, user=self.user)
        self.assertIsNotNone(result, msg='SELECT CMD: No data about table')
        self.check_data(result, [(name, dateofbirth)], cup=False)

    @parameterized.expand(db.neg_change_data)
    def test_error_update_data(self, name, index):
        update_cmd = f"UPDATE {self.table_name} SET Index = {index} WHERE Name = '{name}';"
        select_cmd = f"SELECT Name, DataOfBirth FROM {self.table_name} WHERE Name =  '{name}';"

        result = send_db_command(commands=[update_cmd, select_cmd], db_name=self.db_name, user=self.user)
        self.assertIsNone(result, msg='SELECT CMD: No data about table')
        print(f'Все данные {result} присутствуют')


if __name__ == '__main__':
    unittest.main()
