import pymssql
import pymysql
import config
from read_directory import read_antenna_directory


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
    with open("GET_DATA_FROM_OSS_NSN_4G_CHECK.txt", "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '\n')
    return result
    cursor.close()

def concat_oss_rpdb_data_binary_search_4g(oss_list, rpdb_list):

    concatenated_result = list()

    for oss_item in range(len(oss_list)):

        item = str(oss_list[oss_item][1]) + '0' + str(oss_list[oss_item][2])



        low = 0
        found = False
        high = len(rpdb_list) - 1

        while low <= high and not found:
            mid = (low + high) // 2
            guess = rpdb_list[mid][0]

            if oss_list[oss_item][1] is not None and guess == int(item):
                found = True
                concatenated_result.append(oss_list[oss_item] + rpdb_list[mid])




            elif oss_list[oss_item][1] is not None and guess > int(item):
                high = mid - 1
            elif oss_list[oss_item][1] is not None and guess < int(item):
                low = mid + 1

    with open("CONCATENATED_DATA_4G.txt", "w") as test_file:
        for row in concatenated_result:
            for item in row:
                test_file.write(str(item) + '\t')

    return concatenated_result

def get_oss_data_zte4g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_zte4g)
    result = cursor.fetchall()
    with open("GET_DATA_FROM_OSS_ZTE_4G_CHECK.txt", "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '\n')
            for item in row:
                test_file.write(str(item) + '\t')
    return result
    cursor.close()


def get_oss_data_NSN4G():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_NSN4g)
    result = cursor.fetchall()
    return result
    cursor.close()

def write_lte_data_to_antennas_file_huawei(concatenated_oss_rpdb_data):
    tech = 'LTE'
    with open("Antennas.txt", "a") as antennas_file:
        for row in range(len(concatenated_oss_rpdb_data)):
            antenna_profile_from_rpdb_hua = concatenated_oss_rpdb_data[row][5]
            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_hua + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_hua = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_hua}"

            antennas_file.write(f"{tech}\t"
                                f"{'N/A'}\t"
                                f"{'N/A'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][0])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][6])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][3])}\t"
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][3]) + '/' + '1'}\t"
                                f"{antenna_profile_from_rpdb_hua}\t"  # antenna directory
                                f"{'Huawei'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][6])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][11])}\t" # height
                                f"{'0.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][12])}\t" # azimuth
                                f"{str(concatenated_oss_rpdb_data[row][13])}\t" # mech tilt
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][15])}\t"
                                f"\t"
                                f"{'Overground'}\n")

def write_lte_data_to_antennas_file_zte(concatenated_oss_rpdb_data):
    tech = 'LTE'
    with open("Antennas.txt", "a") as antennas_file:
        for row in range(len(concatenated_oss_rpdb_data)):

            antenna_profile_from_rpdb_zte = str(concatenated_oss_rpdb_data[row][9])
            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_zte + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_zte = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_zte}"

            antennas_file.write(f"{tech}\t"
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][6])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][10])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][11])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][5])}\t" # cell  name
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][5]) + '/' + '1'}\t"
                                f"{antenna_profile_from_rpdb_zte}\t"  # antenna directory
                                f"{'ZTE'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][10])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][11])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][15])}\t" # height
                                f"{str(concatenated_oss_rpdb_data[row][17])}\t" #mechanical downtilt
                                f"{str(concatenated_oss_rpdb_data[row][16])}\t" #azimuth
                                f"{'0.0'}\t"
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][19])}\t" #indoor or outdoor
                                f"\t"
                                f"{'Overground'}\n")


def write_lte_data_to_antennas_file_NSN(concatenated_oss_rpdb_data):
    tech = 'LTE'
    with open("Antennas.txt", "a") as antennas_file:
        for row in range(len(concatenated_oss_rpdb_data)):
            antenna_profile_from_rpdb_nsn = concatenated_oss_rpdb_data[row][6]
            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_nsn + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_nsn = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_nsn}"

            antennas_file.write(f"{tech}\t"
                                f"{str(concatenated_oss_rpdb_data[row][4])}\t" #tac
                                f"{str(concatenated_oss_rpdb_data[row][4])}\t" #tac
                                f"{str(concatenated_oss_rpdb_data[row][0])}\t" #sitename
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t" #id
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t" #id
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][8])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][3])}\t"
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][3]) + '/' + '1'}\t"
                                f"{antenna_profile_from_rpdb_nsn}\t"  # antenna directory
                                f"{'Nokia'}\t" #sector keyword
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][8])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][12])}\t" # height
                                f"{'0.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][13])}\t" # azimuth
                                f"{str(concatenated_oss_rpdb_data[row][14])}\t" # mech tilt
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][16])}\t"
                                f"\t"
                                f"{'Overground'}\n")