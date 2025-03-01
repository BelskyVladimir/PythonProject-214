import ast, json, psycopg2, re

# Класс загрузки исходных параметров и проверки их на корректность.
class LoadParams:
    def __init__(self, file):
        # Загрузка данных для подключений из файла.
        with open(file, 'r') as f:
            json_str = f.read()
        configs = json.loads(json_str)

        self.api_url = configs['api_url']
        self.client = configs['client']
        self.client_key = configs['client_key']
        self.start = configs['start']
        self.end =configs['end']

        self.database = configs['database']
        self.user = configs['user']
        self.password = configs['password']
        self.host = configs['host']
        self.port = configs['port']

        self.smtp_server = configs['smtp_server']
        self.smtp_port = configs['smtp_port']
        self.email_from = configs['email_from']
        self.email_password = configs['email_password']
        self.email_to = configs['email_to']

    def check_params(self):
        # Проверка загруженных данных на соответствие необходимым типам.
        if not isinstance(self.api_url, str):
            return (False, "api url")
        if not isinstance(self.client, str):
            return (False, "client")
        if not isinstance(self.client_key, str):
            return (False, "client_key")
        if not isinstance(self.start, str):
            return (False, "start")
        if not isinstance(self.end, str):
            return (False, "end")
        if not isinstance(self.database, str):
            return (False, "database")
        if not isinstance(self.user, str):
            return (False, "user")
        if not isinstance(self.password, str):
            return (False, "password")
        if not isinstance(self.host, str):
            return (False, "host")
        if not isinstance(self.port, int):
            return (False, "port")
        if not isinstance(self.smtp_server, str):
            return (False, "smtp_server")
        if not isinstance(self.smtp_port, int):
            return (False, "smtp_port")
        if not isinstance(self.email_from, str):
            return (False, "email_from")
        if not isinstance(self.email_password, str):
            return (False, "email_password")
        if not isinstance(self.email_to, str):
            return (False, "email_to")
        # Проверка введенного значения параметра api_url на соответствие шаблону.
        if not re.match(r'https://.+\..+', self.api_url):
            return (False, "api url")
        # Проверка введенного значения параметра start на соответствие шаблону.
        if not re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}', self.start):
            return (False, "start")
        # Проверка введенного значения параметра end на соответствие шаблону.
        if not re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}', self.end):
            return (False, "end")
        # Проверка введенного значения параметра port на допустимость значения.
        if not (self.port >= 1 and self.port <= 65535):
            return (False, "port")
        # Проверка введенного значения параметра smtp_port на допустимость значения.
        if not (self.smtp_port >= 1 and self.smtp_port <= 65535):
            return (False, "smtp_port")
        # Проверка введенного значения параметра email_from на соответствие шаблону.
        if not re.match(r'\S+@\S+\.\S+', self.email_from):
            return (False, "email_from")
        # Проверка введенного значения параметра email_to на соответствие шаблону.
        if not re.match(r'\S+@\S+\.\S+', self.email_to):
            return (False, "email_to")
        return (True,)
# _________________________________________________________________________________

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

# Класс обработки строки данных и загрузки их в базу.
class DataProcessing:
    def __init__(self):
        self.id_uniq = set()
        self.n_all = 0
        self.n_submit = 0
        self.n_correct = 0
        self.query = '''
            insert into grades(
                        user_id,
                        oauth_consumer_key,
                        lis_result_sourcedid,
                        lis_outcome_service_url,
                        is_correct,
                        attempt_type,
                        created_at
                                )
                                values(%s, %s, %s, %s, %s, %s, %s)
                    '''

    # Получение данных из строки-словаря, загрузки их в базу и агрегирование.
    def processing(self, row, dt):
        # Выделение из строки вложенного словаря.
        if isinstance(row['passback_params'], dict):
            passback_params = ast.literal_eval(row['passback_params'])
        else:
            passback_params =dict()

        # Подготовка данных для загрузки в базу.
        self.user_id = row.get('lti_user_id')
        self.oauth_consumer_key = passback_params.get('oauth_consumer_key')
        self.lis_result_sourcedid = passback_params.get('lis_result_sourcedid')
        self.lis_outcome_service_url = passback_params.get('lis_outcome_service_url')
        self.is_correct = row.get('is_correct')
        self.attempt_type = row.get('attempt_type')
        self.created_at = row.get('created_at')

        # Агрегирование данных.
        self.id_uniq.add(self.user_id)
        self.n_all += 1
        if self.attempt_type == 'submit':
            self.n_submit += 1
        if self.is_correct:
            self.n_correct += 1

        self.values = (
            self.user_id,
            self.oauth_consumer_key,
            self.lis_result_sourcedid,
            self.lis_outcome_service_url,
            self.is_correct,
            self.attempt_type,
            self.created_at,
            )

        # Запись данных в базу в соответствии с запросом.
        dt.post(self.query, self.values)

    # Расчеты на основании агрегированных данных.
    def calculate(self, n_hours):
        # Вычисление количества уникальных пользователей.
        self.n_id_uniq = len(self.id_uniq)
        if self.n_id_uniq:
            # Вычисление общего количества запусков кода на одного уникального пользователя.
            self.n_all_per_id_uniq = round(1. * self.n_all / self.n_id_uniq, 2)
            # Вычисление количества отправок решений на проверку на одного уникального пользователя.
            self.n_submit_per_id_uniq = round(1. * self.n_submit / self.n_id_uniq, 2)
            # Вычисление количества правильных решений на одного уникального пользователя.
            self.n_correct_per_id_uniq = round(1. * self.n_correct / self.n_id_uniq, 2)
        else:
            # Избегание деления на ноль в случае отсутствия пользователей.
            self.n_all_per_id_uniq, self.n_submit_per_id_uniq, self.n_correct_per_id_uniq = 0, 0, 0
        if self.n_submit:
            # Вычисление доли правильных решений от количества решений, отправленных на проверку.
            self.success_rate = round(100. * self.n_correct / self.n_submit, 2)
        else:
            # Избегание деления на ноль в случае отсутствия отправленных на проверку решений.
            self.success_rate = 0
        if n_hours:
            # Вычисление среднего количества запусков кода в час.
            self.n_all_per_hour = round(1. * self.n_all / n_hours, 2)
            # Вычисление среднего количества отправок решений на проверку в час.
            self.n_submit_per_hour = round(1. * self.n_submit / n_hours, 2)
            # Вычисление среднего количества верных решений в час.
            self.n_correct_per_hour = round(1. * self.n_correct / n_hours, 2)
        else:
            # Избегание деления на ноль в случае нулевой длительности временного интервала.
            self.n_all_per_hour, self.n_submit_per_hour, self.n_correct_per_hour = 0, 0, 0
# _________________________________________________________________________________


