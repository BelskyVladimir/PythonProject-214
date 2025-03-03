import ast

# Класс обработки строки данных и загрузки их в базу.
class DataProcessing:
    def __init__(self):
        self.id_uniq = set()
        self.n_all = 0
        self.n_submit = 0
        self.n_correct = 0

    # Получение данных из строки-словаря, загрузки их в базу и агрегирование.
    def processing(self, row):
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

        self.values = (
            self.user_id,
            self.oauth_consumer_key,
            self.lis_result_sourcedid,
            self.lis_outcome_service_url,
            self.is_correct,
            self.attempt_type,
            self.created_at,
        )

        # Агрегирование данных.
        self.id_uniq.add(self.user_id)
        self.n_all += 1
        if self.attempt_type == 'submit':
            self.n_submit += 1
        if self.is_correct:
            self.n_correct += 1

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


