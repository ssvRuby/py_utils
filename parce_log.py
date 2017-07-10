import re
from datetime import datetime

'''
Предобработка веб-данных?

1. Набор данных необходимо отфильтровать от записей, генерируемых
автоматически совместно с загрузкой страницы.
2. Удаление записей, не отражающих активность пользователя. Веб-боты в автоматическом
режиме просматривают множество различных страниц в сети. 
3. Определение каждого отдельного пользователя. Большинство порталов в сети Интернет
доступны анонимным пользователям. Можно применять информацию о зарегистрированных 
пользователях, доступные куки-файлы для определения каждого пользователя.
4. Идентификация пользовательской сессии. Это означает, что для каждого визита
определяются страницы, которые был запрошены и их порядок просмотра. 
Также пытаются оценить, когда пользователь покинул веб-сайт.
5. Нахождение полного пути. Множество людей используют кнопку "Назад" для возвращения
к ранее просмотренной странице. Если это происходит, то браузер отображает страницу, 
ранее сохраненную в кэше. Это приводит к "дырам" в журнале веб-сервера. 
Знания о топологии сайта могут быть использованы для восстановления таких пропусков.
'''

'''
 apache format
 '''
# regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) - "(.*?)" "(.*?)"'
# regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" ([0-9]+) ([0-9]+) "(.*?)" "(.*?)"'
regex = '([(\d\.)]+) (.*?) (.*?) \[(.*?)\] "(.*?)" ([0-9]+) ([0-9]+) "(.*?)" "(.*?)"'
with open('20170628_access.log', 'r') as log_file:
    all_lines = 0
    pic = 0
    robot = 0

    for line in log_file:
        all_lines += 1
        logs_str = list(re.match(regex, line).groups())

# =========== Фильтры ========================================================================
# Картинки:
        if '.gif' in logs_str[4] or '.jpeg' in logs_str[4] or '.js' in logs_str[4] \
                or '.css' in logs_str[4]:
            pic += 1
            continue
# роботы, боты:
        if '+http' in logs_str[8] or 'robot' in logs_str[8] or 'Riddler' in logs_str[8] \
                or 'Bot' in logs_str[8] or 'bot' in logs_str[8]or 'bots' in logs_str[8]:
            robot += 1
            continue
# ============================================================================================

# Преобразование даты/времени в объект datatime
        logs_str[3] = datetime.strptime(logs_str[3], '%d/%b/%Y:%H:%M:%S %z')

print('all =====>', all_lines, '  pic ===== >', pic, '  pic (%) ===== >', pic*100//all, '%', ' robot ===== >', robot)