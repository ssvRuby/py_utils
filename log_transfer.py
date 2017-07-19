import sh
import os
import datetime
import time
import re
import gzip
import cx_Oracle
from datetime import datetime as dt

'''
VERSION 1.0

'''

oracle_user_name = 'BDATA'
oracle_user_password = 'oracle'
oracle_ip = '172.25.100.212'
oracle_sid = 'wla'

REMOTE_WEB_SERVER = 'root@109.68.190.75'
SOURCE_DIR = '/var/log/nginx/'
DESTINATION_DIR = '/sv_weblogs/vtb24leasing/'
DATE_SHIFT = 1
NAMES_PREFIX = 'acc'
NAMES_POSTFIX = '_access.log.gz'

destination_file_name = ''


def get_dateprefix(lt):
    return "{0}{1}{2}".format(str(lt.tm_year), (str(lt.tm_mon) if lt.tm_mon > 9 else '0' + str(lt.tm_mon)),
                              str(lt.tm_mday))


def transfer_log():
    yesterday = time.localtime(time.time() - 86400 * DATE_SHIFT)
    source_date = datetime.date(yesterday.tm_year, yesterday.tm_mon, yesterday.tm_mday)
    source_month = source_date.strftime('%b')
    destination_file_name = get_dateprefix(yesterday) + NAMES_POSTFIX
    remote_server = sh.ssh.bake(REMOTE_WEB_SERVER)
    file_list = remote_server.ls('-l {}'.format(SOURCE_DIR)).stdout.decode('utf-8').split('\n')
    for filename in file_list[1:-1]:
        fa = filename.split()
        if NAMES_PREFIX in fa[8]:
            curr_day, curr_month = fa[6], fa[5]
            if curr_month == source_month and curr_day == str(yesterday.tm_mday):
                source_file_name = fa[8]
                break
    os.system('scp {}:{}{} {}{}'.format(REMOTE_WEB_SERVER, SOURCE_DIR, source_file_name,
                                        DESTINATION_DIR, destination_file_name))
    return destination_file_name


def get_filtered_records(access_log_file_name, test_mode=False, test_file_name=''):
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

            logs_str.append(dt.strptime(logs_str[3], '%d/%b/%Y:%H:%M:%S %z'))
            result_lst.append(logs_str)

    if test_mode:
        with open(test_file_name, 'w') as prepared_log:
            for r in result_lst:
                prepared_log.write(str(r) + '\n')

    return result_lst


def str_to_sql(literal):
    return '\'{}\''.format(literal)


def connect_to_db(sysdba=False):
    try:
        if sysdba:
            return cx_Oracle.connect('sys/oracle@172.25.100.212/wla', mode=cx_Oracle.SYSDBA)
        else:
            return cx_Oracle.connect(
                '{}/{}@{}/{}'.format(oracle_user_name, oracle_user_password, oracle_ip, oracle_sid))

    except cx_Oracle.Error as ora_ex:
        error, = ora_ex.args
        print("Oracle-Error-Code:", error.code)
        print("Oracle-Error-Message:", error.message)
        return False


def close_connect_to_db(conn):
    if conn:
        conn.close()


def write_to_oracle(filtered_records):
    if len(filtered_records) > 0:

        conn = connect_to_db()

        if conn:

            try:
                cursor = conn.cursor()
                cursor.prepare('INSERT INTO RAW_LOG (ID, IP, REQ_IDENTITY, REQ_USER_ID, REQ_DATE, REQ_PAGE, \
                                                     REQ_CODE, REQ_SIZE, REQ_REFER, REQ_AGENT, T_STAMP)     \
                                VALUES (RAW_LOG_SEQ.NEXTVAL, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')

                cursor.executemany(None, filtered_records)
                conn.commit()
                close_connect_to_db(conn)

            except cx_Oracle.DatabaseError as ora_ex:
                error, = ora_ex.args
                print("Oracle-Error-Code:", error.code)
                print("Oracle-Error-Message:", error.message)


def get_log_file_name(d_file_name):

    try:

        gzf = gzip.open(d_file_name, 'r')
        file_content = gzf.read()

        unpacked_file = DESTINATION_DIR + 'ssv.log'
        with open(unpacked_file, 'wb') as log_file:
            log_file.write(file_content)

    except Exception as ex:
        unpacked_file = ''
        error, = ex.args
        print("Error-Code:", ex.args)
        print("Error-Message:", error)

    return unpacked_file


destination_file_name = transfer_log()
log_file_name = get_log_file_name(DESTINATION_DIR + destination_file_name)

if log_file_name != '':
    filtered_records = get_filtered_records(log_file_name, test_mode=False, test_file_name='prepared_log')
    write_to_oracle(filtered_records)






