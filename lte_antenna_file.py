import pymssql
import pymysql
import config

def get_rpdb_data_4g():
    conn = pymssql.connect(host=config.host_mssql, user=config.user_mssql, password=config.password_mssql,
                           database=config.db_mssql)
    c = conn.cursor()
    c.execute(config.query_rpdb_mentor4g)
    result_set = c.fetchall()
    def sortSecond(val):
        return val[5]
    result_set.sort(key=sortSecond)
    return result_set
    c.close()


def get_oss_data_huawei4g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_hua4g)
    result = cursor.fetchall()
    return result
    cursor.close()

def concat_oss_rpdb_data_binary_search_huawei4g(oss_list, rpdb_list):
    low = 0
    high = len(rpdb_list)-1
    mid = (low + high ) / 2
    guess = rpdb_list[mid]


    for oss_item in range(len(oss_list)):
        item = oss_list[oss_item][1] + '0' + str(oss_list[oss_item][2])
        print(item)

    print(high)
    print(mid)
    print(guess)

