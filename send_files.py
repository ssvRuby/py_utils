# ==========================================================================================
# отправка файла-журнала на FTP    ==SSV== version 1.0
# ==========================================================================================
import os.path as osp
import os
import time
import ftplib


def get_dateprefix(lt):
    return "{0}{1}{2}".format(str(lt.tm_year), (str(lt.tm_mon) if lt.tm_mon > 9 else '0' + str(lt.tm_mon)),
                              str(lt.tm_mday))


def send_file_to_ftp():
    ftp_conn = ftplib.FTP()
    ftp_conn.connect(ftp_host, ftp_port)
    ftp_conn.login(ftp_user, ftp_password)
    f = open(logs_dir + log_for_export, "rb")
    ftp_conn.storbinary("STOR " + new_file_name, f)
    ftp_conn.close()


''' == FTP-parameters        =============================================
ftp_host 
ftp_port
ftp_user
ftp_password
'''

ftp_host = "lsg-fs02.sl24.internal"
ftp_port = 2221
ftp_user = "sl24ftp1"
ftp_password = "eybrfkmysq"

''' == Log dir parameters ===============================================
 logs_dir     - log file directory
 data_shift   - смещение назад, в днях
 names_prefix - часть имени файла, для идентификации
'''

logs_dir = 'F:\\Logs\\'
data_shift = 5
yesterday = time.localtime(time.time() - 86400 * data_shift)
names_prefix = 'acc'
names_postfix = '_access.log.gz'
log_for_export = ''
new_file_name = ''
log_file_names = os.listdir(logs_dir)

for file in log_file_names:
    loc_time = time.localtime(osp.getatime(logs_dir + file))

    if names_prefix in file and loc_time.tm_mon == yesterday.tm_mon and loc_time.tm_mday == yesterday.tm_mday:
        log_for_export = file
        new_file_name = get_dateprefix(loc_time) + names_postfix
        break

if log_for_export:
    send_file_to_ftp()
