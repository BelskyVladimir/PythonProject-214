import psycopg2

# Класс для работы с базой данных по шаблону Singleton.
class DatabaseConnection:
    # Проверка наличия существующего подключения к базе данных.
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance
    # Создание подключения к базе данных в случае отсутствия имеющегося подключения.
    def __init__(self, host, port, database, user, password, autocommit=False):
        self.connection = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
        if autocommit:
            self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    # Запрос на выборку данных из базы.
    def select(self, query, vars):
        self.cursor.execute(query, vars)
        res = self.cursor.fetchall()
        return res

    # Запрос на изменение данных в базе.
    def post(self, query, vars):
        self.cursor.execute(query, vars)
        if not self.connection.autocommit:
            self.connection.commit()

    # Отключение соединения.
    def exit(self):
        self.cursor.close()
        self.connection.close()
# _________________________________________________________________________________
