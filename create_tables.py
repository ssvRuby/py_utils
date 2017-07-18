import cx_Oracle

'''
#--ACCESS_LOG TABLE ---------------------------------------
id                                       NUMBER         PK
tstamp                                   DATE
site                                     NUMBER         FK
#----------------------------------------------------------
ip        ip адрес (%h)                  VARCHAR2(15)
identity  RFC 1413 identity (%l)         VARCHAR2(100)
userid    userid (%u)                    VARCHAR2(100)
date      дата/время (%t)                VARCHAR2(30) 
page      запрашиваемая страница (%r)    VARCHAR2(1500)
code      код статуса (%>s)              VARCHAR2(3)
size      размер (%b)                    VARCHAR2(12)
referer   источник                       VARCHAR2(2500)
agent     пользовательский агент         VARCHAR2(1500)
#----------------------------------------------------------
'''

oracle_user_name = 'BDATA'
oracle_user_password = 'oracle'
oracle_ip = '172.25.100.212'
oracle_sid = 'wla'

create_table = """CREATE TABLE RAW_LOG (
                        ID                      NUMBER NOT NULL ENABLE,
                        RECORDS_DATE            DATE DEFAULT sysdate NOT NULL ENABLE, 
                        T_STAMP                 DATE,
                        IP                      VARCHAR2(15),
                        REQ_IDENTITY            VARCHAR2(100),
                        REQ_USER_ID             VARCHAR2(100),
                        REQ_DATE                VARCHAR2(30), 
                        REQ_PAGE                VARCHAR2(1500),
                        REQ_CODE                VARCHAR2(3),
                        REQ_SIZE                VARCHAR2(12),
                        REQ_REFER               VARCHAR2(2500),
                        REQ_AGENT               VARCHAR2(1500),
                        CONSTRAINT "RAW_LOG_PK" PRIMARY KEY ("ID")
                        USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255
                        )
"""

create_sequence = '''CREATE SEQUENCE  RAW_LOG_SEQ
                        MINVALUE 1 MAXVALUE 9999999999999999999999999999 
                        INCREMENT BY 1 
                        START WITH 1 
                        CACHE 20 NOORDER  NOCYCLE  NOPARTITION
'''


def get_check_seq_str(owner, sequence_name):
    return '''SELECT count(*) FROM all_objects 
              WHERE object_type = 'SEQUENCE' 
              AND object_name = '{}'
              AND owner = '{}'
           '''.format(sequence_name, owner)


def get_check_table_str(owner, table_name):
    return '''SELECT count(*) FROM all_objects
              WHERE object_type in ('TABLE','VIEW')
              AND object_name = '{}'
              AND owner = '{}'
           '''.format(table_name, owner)


def object_exist(cursor, check_string):
    obj_ex = False
    result = cursor.execute(check_string).fetchone()
    if len(result) > 0:
        if result[0] > 0:
            obj_ex = True
    return obj_ex


try:

    conn = cx_Oracle.connect('{}/{}@{}/{}'.format(oracle_user_name, oracle_user_password, oracle_ip, oracle_sid))
    cursor = conn.cursor()

    if not object_exist(cursor, get_check_seq_str(oracle_user_name, 'RAW_LOG_SEQ')):
        cursor.execute(create_sequence)

    if not object_exist(cursor, get_check_table_str(oracle_user_name, 'RAW_LOG')):
        cursor.execute(create_table)

    conn.commit()
    conn.close()

except cx_Oracle.DatabaseError as ora_ex:
    error, = ora_ex.args
    print("Oracle-Error-Code:", error.code)
    print("Oracle-Error-Message:", error.message)
