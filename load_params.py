import json, re

# Класс загрузки исходных параметров и проверки их на корректность.
class LoadParams:
    def __init__(self, file):
        # Загрузка данных для подключений из файла.
        with open(file, 'r') as f:
            json_str = f.read()
        self.configs = json.loads(json_str)

    def check_params(self):
        # Проверка загруженных данных на соответствие необходимым типам и допустимость значений.
        for key, value in self.configs.items():
            #print(key, value)
            if 'port' in key:
                print(key, value)
                if not isinstance(value, int):
                    return (False, key)
                if not (value >= 1 and value <= 65535):
                    return (False, key)
            else:
                print(key, value)
                if not isinstance(value, str):
                    return (False, key)
            print('OK')

        # Проверка введенного значения параметра api_url на соответствие шаблону.
        if not re.match(r'https://.+\..+', self.configs['api_url']):
            return (False, "api url")

        # Проверка введенного значения параметров start и stop на соответствие шаблону.
        for key in ['start', 'end']:
            if not re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}', self.configs[key]):
                return (False, key)

        # Проверка введенного значения параметра email_from и mail_to на соответствие шаблону.
        for key in ['email_from', 'email_to']:
            if not re.match(r'\S+@\S+\.\S+', self.configs[key]):
                return (False, key)

        return (True,)
# _________________________________________________________________________________
