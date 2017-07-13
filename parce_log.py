import re
import cx_Oracle

from datetime import datetime

'''
    Очистка журнала от неинформативных записей


1. Фильтрация от записей, генерируемых автоматически совместно с загрузкой страницы.
2. Удаление записей, не отражающих активность пользователя. 
3. Определение каждого отдельного пользователя (если есть аутентификация). 
4. Идентификация пользовательской сессии - для каждого визита определяются страницы,
   которые был запрошены и их порядок просмотра. 


  # Преобразование даты/времени в объект datatime
                    # logs_str[3] = datetime.strptime(logs_str[3], '%d/%b/%Y:%H:%M:%S %z')
                    # 20170628_access.log
'''


def get_filtered_records(access_log_file_name, test_mode=False, test_file_name = ''):

    result_lst = []
    regex = '([(\d\.)]+) (.*?) (.*?) \[(.*?)\] "(.*?)" ([0-9]+) ([0-9]+) "(.*?)" "(.*?)"'

    with open(access_log_file_name, 'r') as log_file:
        for line in log_file:
            logs_str = list(re.match(regex, line).groups())
            # =========== Фильтры =============================================================================
            # Картинки:
            if '.gif' in logs_str[4] or '.jpeg' in logs_str[4] or '.js' in logs_str[4] \
                    or '.ico' in logs_str[4] or '.png' in logs_str[4] or '.css' in logs_str[4] \
                    or '.jpg' in logs_str[4] or '.JPG' in logs_str[4] or 'vtb24.fonts' in logs_str[4] \
                    or '/autodiscover' in logs_str[4] or '/bitrix/admin' in logs_str[4]:
                continue
            if '.gif' in logs_str[7] or '.jpeg' in logs_str[7] or '.js' in logs_str[7] \
                    or '.ico' in logs_str[7] or '.png' in logs_str[7] or '.css' in logs_str[7] \
                    or '.jpg' in logs_str[7] or logs_str[7] == '-':
                continue
            # оботы, боты:
            if '+http' in logs_str[8] or 'robot' in logs_str[8] or 'Riddler' in logs_str[8] \
                    or 'Bot' in logs_str[8] or 'bot' in logs_str[8] or 'bots' in logs_str[8] \
                    or 'http://dsp.cubo.ru/' in logs_str[7]:
                continue
            if logs_str[5] == '499' and logs_str[6] == '0':
                continue
            # реферы с собственных страниц
            if ('vtb24leasing.ru/' in logs_str[7] and 'public_session.php?' in logs_str[4]) \
                    or ('//vtb24leasing.ru/' in logs_str[7] and 'get_sections.php' in logs_str[4]) \
                    or ('//vtb24leasing.ru/' in logs_str[7] and 'POST' in logs_str[4]):
                continue
            # реферы CUBO
            if 'cubo&utm' in logs_str[4] or 'cubo&utm' in logs_str[7]:
                continue
            # Какая-то оставшаяся хуйня
            if 'GET /bitrix/tools/public_session.php?' in logs_str[4]:
                continue
            # ===============================================================================================

            result_lst.append(logs_str)

    if test_mode:
        with open(test_file_name, 'w') as prepared_log:
            for r in result_lst:
                prepared_log.write(str(r) + '\n')

    return result_lst


def str_to_sql(literal):
    return '\'{}\''.format(literal)



def connect_to_db():
    try:
        return cx_Oracle.connect('sys/oracle@172.25.100.212/wla', mode=cx_Oracle.SYSDBA)

    except cx_Oracle.Error as ora_ex:
        error, = ora_ex.args
        print("Oracle-Error-Code:", error.code)
        print("Oracle-Error-Message:", error.message)
        return False


def close_connect_to_db(conn):
    if conn:
        conn.close()

# filtered_records = get_filtered_records('20170628_access.log', True, 'prepared_log')

# print('=====> Try connect to DB!')
# if len(filtered_records) > 0:
#     conn = connect_to_db()
#
#     if conn:
#         print(conn.version.split("."))
#         # transaction
#         for r in filtered_records:
#             pass  # insert
#         # commit conn.commit()
#         close_connect_to_db(conn)

# for r in filtered_records:
#     print(r)
# print(len(filtered_records))

conn = connect_to_db()
print(conn.version.split("."))
cursor = conn.cursor()





try:
    insert_string = 'INSERT INTO ssv (id, status, tstamp) ' \
                    'values (1, {}, TO_DATE({}, {}))'.format(str_to_sql('dddddd'),
                                                             str_to_sql('01-09-1988'), str_to_sql('DD.MM.YYYY'))
    print(insert_string)
    cursor.prepare(insert_string)
    print(cursor.statement)
    cursor.execute(None)
    conn.commit()
    # for result in cursor:
    #     # dddd, = result
    #     # print('ddd ', type(dddd))
    #     print('=====> ', type(result))
    #     print('=====> ', result)
    #     for r in result:
    #         print(r)
    #     aaa = 1
except cx_Oracle.DatabaseError as ora_ex:
    error, = ora_ex.args
    print("Oracle-Error-Code:", error.code)
    print("Oracle-Error-Message:", error.message)


close_connect_to_db(conn)
