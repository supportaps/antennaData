import pymssql
import pymysql
import config

def get_data_from_rpdb_2G():

    conn_db = pymssql.connect(host=config.host_mssql, user=config.user_mssql, password=config.password_mssql,
                              database=config.db_mssql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_rpdb_mentor)
    result_list_rpdb = []
    result_set = cursor.fetchall()

    with open("GET_DATA_FROM_RPDB_2G.txt", "w") as test_file:
        for row in result_set:

            if row[1][-2:] == '_D' or row[1][-2:] == '_G' or row[0] is not None:


                result_list_rpdb.append(row)


        def sortSecond(val):
            rnc_id_nonetype_check = val[0]
            if rnc_id_nonetype_check is None:
                rnc_id_nonetype_check = 0
            return rnc_id_nonetype_check + val[2] + val[3]

        result_list_rpdb.sort(key=sortSecond)
        for row in result_list_rpdb:
            test_file.write(str(row) + '\n')
        return result_list_rpdb
        cursor.close()

def get_oss_data_huawei_2g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_hua_2g)
    result = cursor.fetchall()
    with open("GET_DATA_FROM_OSS_HUAWEI_2G_CHECK.txt", "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + '\n')
    return result
    cursor.close()

def concat_oss_rpdb_data_binary_search_2g(oss_list, rpdb_list):


    concatenated_result = list()

    for oss_item in range(len(oss_list)):

        item = int(oss_list[oss_item][0]) + int(oss_list[oss_item][2]) + int(oss_list[oss_item][3])

        print("Item^ ", item)

        low = 0
        found = False
        high = len(rpdb_list) - 1

        while low <= high and not found:
            mid = (low + high) // 2
            print("mid: ", mid)
            guess_controller = rpdb_list[mid][0]
            print("guess_controller: ", guess_controller)
            if guess_controller is None:
                guess_controller = 0
            guess = guess_controller + rpdb_list[mid][2] + rpdb_list[mid][3]
            print("guess: ", guess)
            print("Item^ ", item)

            if oss_list[oss_item][0] is not None and int(guess) == int(item):
                found = True
                concatenated_result.append(oss_list[oss_item] + rpdb_list[mid])
                print("FOUND: ", found)






            elif oss_list[oss_item][0] is not None and int(guess) > int(item):

                high = mid - 1

                print("high: ", high)

            elif oss_list[oss_item][0] is not None and int(guess) < int(item):

                print("high: ", low)



    with open("CONCATENATED_DATA_2G.txt", "w") as test_file:
        for row in concatenated_result:
            for item in row:
                test_file.write(str(item) + '\t')

    return concatenated_result

def write_gsm_huawei_to_coordinates_file(concatenated_data):
    with open("written_DATA_2G.txt", "w") as test_file:
        for row in concatenated_data:
            for item in row:
                test_file.write(str(item) + '\t')



