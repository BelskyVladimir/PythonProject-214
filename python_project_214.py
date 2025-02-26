import os, ast, json
import logging, requests, psycopg2
import gspread, smtplib, ssl
from datetime import datetime, timedelta
from glob import glob
from email.message import EmailMessage
from oauth2client.service_account import ServiceAccountCredentials

# Текущая дата в строковом формате.
DATA = datetime.now().strftime('%Y-%m-%d')

# Настройка конструктора логирования.
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO,
    filename=f'project214_log_{DATA}.log',
    filemode='a'
)

try:
    # Определение класса для работы с базой данных по шаблону Singleton.
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


    # Класс обработки строки данных и загрузки их б базу.
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
    # _________________________________________________________________________________


    # Определение функции отправки сообщения по электронной почте.
    def send_email(message):
        # Создание контекста SSL по умолчанию
        context = ssl.create_default_context()
        msg = EmailMessage()
        subject = f'Loading data on {datetime.strftime(START, '%Y-%m-%d')}.'
        # Формирование сообщения.
        msg.set_content(message)
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        # Подключение к SMTP серверу.
        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context)
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        # Отправка сообщения.
        server.send_message(msg=msg)
        # Отключение сервера.
        server.quit()
    # _________________________________________________________________________________


    # Запись лога о начале работы программы.
    logging.info('Getting started with the program.')

    # Удаление лог-файлов старше двух дней.
    # Считывание названий лог-файлов из текущей директории.
    dir_path = os.getcwd()
    files_log = list(glob(os.path.join(dir_path, '*.log')))
    # Определение текущей даты.
    data_now = datetime.now().date()
    # Поиск и удаление лог-файлов старше двух дней от текущей даты.
    for file in files_log:
        data_file = datetime.strptime(file[-14:-4], '%Y-%m-%d').date()
        if (data_now - data_file) >= timedelta(days=3):
            os.remove(file)
    logging.info('Устаревшие лог-файлы удалены.')


    # Загрузка данных для подключений из файла.
    with open('params.txt', 'r') as f:
        json_str = f.read()
    configs = json.loads(json_str)

    # Вводные данные для подключения к API обучающей системы.
    API_URL = configs['api_url']
    CLIENT = configs['client']
    CLIENT_KEY = configs['client_key']
    START = datetime.strptime(configs['start'], '%Y-%m-%d %H:%M:%S.%f')
    END = datetime.strptime(configs['end'], '%Y-%m-%d %H:%M:%S.%f')
    if START > END:
        raise ValueError(
            f'The start value of the period is later than the end value of the period: start = {START}, end = {END}.')

    # Вводные данные для подключения к базе данных.
    DATABASE = configs['database']
    USER = configs['user']
    PASSWORD = configs['password']
    HOST = configs['host']
    PORT = configs['port']

    # Вводные данные для подключения к почтовому сервису.
    SMTP_SERVER = configs['smtp_server']
    SMTP_PORT = configs['smtp_port']
    EMAIL_FROM = configs['email_from']
    EMAIL_PASSWORD =configs['email_password']
    EMAIL_TO = configs['email_to']
    # Запись об успешной загрузке необходимых данных для подключений.
    logging.info('Data for connection are loaded')

    # Подключение к базе данных.
    dt = DatabaseConnection(HOST, PORT, DATABASE, USER, PASSWORD)
    logging.info('Database connection is completed successfully.')

    # Создание экземпляра класса для извлечения, агрегирования данных и записи их в базу.
    process = DataProcessing()
    # Вычисление длительности выбранного временного интервала.
    n_hours = (END - START).total_seconds() / 3600. # Длительность выбранного интервала в часах.

    # Разбиение выбранного временного интервала на часовые промежутки для уменьшения нагрузки.
    start_time = START
    while start_time < END:
        # Укорачивание длины промежутка, если она выходит за пределы выбранного интервала.
        end_time = END if END <= start_time + timedelta(hours=1) else start_time + timedelta(hours=1)

        # Выполнение запроса к API.
        logging.info(f'Sending a request for data in the range from {start_time} to {end_time}.')
        params = {'client':CLIENT,
                  'client_key':CLIENT_KEY,
                  'start':datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S.%f'),
                  'end':datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S.%f')}
        responce = requests.get(API_URL, params=params)
        logging.info('The request was completed successfully.')

        # Обработка полученных данных.
        for row in responce.json():
            process.processing(row, dt)
        # Запись об успешной загрузке данных за временной интервал в базу.
        logging.info('Data is successfully loaded into the database.')
        # Переход к новому временному интервалу.
        start_time = end_time

    # Отключение базы данных.
    dt.exit()
    logging.info('All data for the selected period has been loaded into the database.'
                 ' The connection to the database was interrupted.')

    # Расчеты на основании агрегированных данных.
    # Вычисление количества уникальных пользователей.
    n_id_uniq = len(process.id_uniq)
    if n_id_uniq:
        # Вычисление общего количества запусков кода на одного уникального пользователя.
        n_all_per_id_uniq = round(1. * process.n_all / n_id_uniq, 2)
        # Вычисление количества отправок решений на проверку на одного уникального пользователя.
        n_submit_per_id_uniq = round(1. * process.n_submit / n_id_uniq, 2)
        # Вычисление количества правильных решений на одного уникального пользователя.
        n_correct_per_id_uniq = round(1. * process.n_correct / n_id_uniq, 2)
    else:
        # Избегание деления на ноль в случае отсутствия пользователей.
        n_all_per_id_uniq, n_submit_per_id_uniq, n_correct_per_id_uniq = 0, 0, 0
    if process.n_submit:
        # Вычисление доли правильных решений от количества решений, отправленных на проверку.
        success_rate = round(100. * process.n_correct / process.n_submit, 2)
    else:
        # Избегание деления на ноль в случае отсутствия отправленных на проверку решений.
        success_rate = 0
    if n_hours:
        # Вычисление среднего количества запусков кода в час.
        n_all_per_hour = round(1. * process.n_all / n_hours, 2)
        # Вычисление среднего количества отправок решений на проверку в час.
        n_submit_per_hour = round(1. * process.n_submit / n_hours, 2)
        # Вычисление среднего количества верных решений в час.
        n_correct_per_hour = round(1. * process.n_correct / n_hours, 2)
    else:
        # Избегание деления на ноль в случае нулевой длительности временного интервала.
        n_all_per_hour, n_submit_per_hour, n_correct_per_hour = 0, 0, 0
    logging.info('Data has been aggregated successfully.')

    # Подключение к Google Таблицам.
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name("gs_credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open("project214_result").sheet1
    logging.info('Connection to Google Sheets is complete.')

    # Запись агрегированных данных в гугл таблицу.
    sheet.append_row([datetime.strftime(START, '%Y-%m-%d %H:%M:%S.%f'),
                            datetime.strftime(END, '%Y-%m-%d %H:%M:%S.%f'),
                            n_id_uniq, process.n_all, process.n_submit, process.n_correct, success_rate,
                            n_all_per_hour, n_submit_per_hour, n_correct_per_hour,
                            n_all_per_id_uniq, n_submit_per_id_uniq, n_correct_per_id_uniq])
    logging.info('Aggregated data written to Google Sheets successfully')

    # Отправка информационного сообщения по электронной почте.
    message = f'''
        Data loading for the period from {datetime.strftime(START, '%Y-%m-%d %H:%M:%S.%f')}\
        to {datetime.strftime(END, '%Y-%m-%d %H:%M:%S.%f')} completed successfully.
        '''
    send_email(message)
    logging.info('Email message sent.')


except requests.exceptions.HTTPError as err:
    logging.error(err, exc_info=True)
    # Отправка информационного сообщения по электронной почте.
    message = f'''
            Problems arose when connecting to the training system via API: \n\n{err}.
                '''
    send_email(message)
    logging.info('Email message sent.')

except Exception as err:
    logging.error(err, exc_info=True)
    # Отправка информационного сообщения по электронной почте.
    message = f'''
            An error occurred while running the program: \n\n{err}.
            '''
    send_email(message)
    logging.info('Email message sent.')


