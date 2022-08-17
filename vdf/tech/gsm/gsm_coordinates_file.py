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

    with open(config.path_test7, "w") as test_file:
        for row in result_set:
            print(row[1][-2:])

            if row[1][-2:] == "_D" or row[1][-2:] == "_G" or row[0] is not None and row[1][-2:] != "_U":
                result_list_rpdb.append(row)

        def sortSecond(val):
            rnc_id_nonetype_check = val[0]
            if rnc_id_nonetype_check is None:
                rnc_id_nonetype_check = 0
            return str(rnc_id_nonetype_check) + str(val[2]) + str(val[3])

        result_list_rpdb.sort(key=sortSecond)
        for row in result_list_rpdb:
            test_file.write(str(row) + '\n')
        return result_list_rpdb
        cursor.close()

def get_oss_data_zte_2g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_zte_2g)
    result = cursor.fetchall()
    with open(config.path_test12, "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + '\n')
    return result
    cursor.close()

def get_oss_data_huawei_2g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_hua_2g)
    result = cursor.fetchall()
    with open(config.path_test8, "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + '\n')
    return result
    cursor.close()

def get_oss_data_nokia_2g():
    conn_db = pymysql.connect(host=config.host_mysql_v, user=config.user_mysql_v, password=config.password_mysql_v,
                              db=config.db_mysql_v)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_nokia_2g)
    result = cursor.fetchall()
    #with open(config.path_test8, "w") as test_file:
        #for row in result:
            #test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + '\n')
    return result
    cursor.close()

def concat_oss_rpdb_data_binary_search_2g(oss_list, rpdb_list):
    concatenated_result = list()

    for oss_item in range(len(oss_list)):

        item = str(oss_list[oss_item][0]) + str(oss_list[oss_item][2]) + str(oss_list[oss_item][3])

        low = 0
        found = False
        high = len(rpdb_list) - 1

        while low <= high and not found:
            mid = (low + high) // 2

            guess_controller = rpdb_list[mid][0]

            if guess_controller is None:
                guess_controller = 0
            guess = str(guess_controller) + str(rpdb_list[mid][2]) + str(rpdb_list[mid][3])

            if oss_list[oss_item][0] is not None and str(guess) == str(item):
                found = True
                concatenated_result.append(oss_list[oss_item] + rpdb_list[mid])







            elif oss_list[oss_item][0] is not None and str(guess) > str(item):

                high = mid - 1



            elif oss_list[oss_item][0] is not None and str(guess) < str(item):

                low = mid + 1

    with open(config.path_test9, "w") as test_file:
        for row in concatenated_result:
            test_file.write(str(row) + '\n')

    return concatenated_result


def write_gsm_huawei_to_coordinates_file(concatenated_data):
    head = 'Sector	Latitude	Longitude	Azimuth	LAC	CI'

    with open(config.remote_coordinates_file_path, "w") as test_file:
        test_file.write(head + '\n')

        for row in concatenated_data:
            test_file.write(f"{row[1]}\t"  # sector name
                            f"{str(row[11])}\t"  # long
                            f"{str(row[12])}\t"  # lat
                            f"{str(row[14])}\t"  # az
                            f"{row[2]}\t"  # lac
                            f"{row[3]}\n")  # ci

def write_gsm_nokia_to_coordinates_file(concatenated_data):

    with open(config.remote_coordinates_file_path, "a") as test_file:

        for row in concatenated_data:
            test_file.write(f"{row[1]}\t"  # sector name
                            f"{str(row[11])}\t"  # long
                            f"{str(row[12])}\t"  # lat
                            f"{str(row[14])}\t"  # az
                            f"{row[2]}\t"  # lac
                            f"{row[3]}\n")  # ci

def write_gsm_zte_to_coordinates_file(concatenated_data):

    with open(config.remote_coordinates_file_path, "a") as test_file:

        for row in concatenated_data:
            test_file.write(f"{row[1]}\t"  # sector name
                            f"{str(row[11])}\t"  # long
                            f"{str(row[12])}\t"  # lat
                            f"{str(row[14])}\t"  # az
                            f"{row[2]}\t"  # lac
                            f"{row[3]}\n")  # ci

def write_gsm_huawei_to_antenna_file(concatenated_data):
    head = 'Sector	Antenna Model	Electrical Tilt	Mechanical Tilt	Height	Azimuth'

    with open(config.remote_antennas2g_file_path, "w") as test_file:
        test_file.write(head + '\n')

        for row in concatenated_data:
            test_file.write(
                row[1] + '\t' + str(row[10]) + '\t' + str(row[16]) + '\t' + str(row[15]) + '\t' + row[13] + '\t' + row[
                    14] + '\n')
