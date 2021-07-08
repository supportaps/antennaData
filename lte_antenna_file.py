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
        return val[0]
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

    concatenated_result = list()

    for oss_item in range(len(oss_list)):
        item = oss_list[oss_item][1] + '0' + str(oss_list[oss_item][2])
        low = 0
        found = False
        high = len(rpdb_list) - 1

        while low <= high and not found:
            mid = (low + high) // 2
            guess = rpdb_list[mid][0]
            if guess == int(item):
                found = True
                concatenated_result.append(oss_list[oss_item] + rpdb_list[mid])
            elif guess > int(item):
                high = mid - 1
            else:
                low = mid + 1

    return concatenated_result