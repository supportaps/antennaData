import pymssql
import pymysql
import config
from read_directory import read_antenna_directory
from vdf.tech.lte import lte_util


def get_rpdb_data_4g():
    conn = pymssql.connect(host=config.host_mssql, user=config.user_mssql, password=config.password_mssql,
                           database=config.db_mssql)
    c = conn.cursor()
    c.execute(config.query_rpdb_mentor4g)
    result_set = c.fetchall()
    def sortSecond(val):
        return val[0]
    result_set.sort(key=sortSecond)
    print(result_set)
    with open(config.path_test1, "w") as test_file:
        for row in result_set:
           test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + str(row[4]) + ' | ' + str(row[5]) + ' | ' + str(row[6]) + ' | ' + str(
                row[7]) + str(row[8]) + ' | ' + str(row[9]) + ' | ' + str(row[10]) + ' | ' + str(
                row[11]) + '\n')
    return result_set
    c.close()
    print("GOT  RPDB FOR 4G")


def get_oss_data_huawei4g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_hua4g)
    result = cursor.fetchall()
    with open("../../../LTE_GET_DATA_FROM_RPDB_NSN_4G_CHECK.txt", "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '\n')
    return result
    cursor.close()

def concat_oss_rpdb_data_binary_search_4g(oss_list, rpdb_list):

    concatenated_result = list()
    item = 0
    for oss_item in range(len(oss_list)):


        if len(str(oss_list[oss_item][2])) == 2:
            item = str(oss_list[oss_item][1]) + '0' + str(oss_list[oss_item][2])
        else:
            item = str(oss_list[oss_item][1]) + str(oss_list[oss_item][2])



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



    with open(config.path_test2, "w") as test_file:
        for row in concatenated_result:
            for item in row:
                test_file.write(str(item) + '\t')



    return concatenated_result
    print("DATA NSN IS CONCATENATED")


def change_nodeb_name_nsn(conctenated_nsn_list, nodebdata_list):
    res_list = list()
    with open(config.path_test3, "w") as test_file:
        for nodeb_oss in range(len(conctenated_nsn_list)):

            cell_list = list(conctenated_nsn_list[nodeb_oss])


            for nodeb_from_bts_table in range(len(nodebdata_list)):

                nodeb_list = list(nodebdata_list[nodeb_from_bts_table])


                if cell_list[1] == nodeb_list[2]:
                    true_bts = nodeb_list[1]
                    cell_list[0] = true_bts

                    res_list.append(cell_list)
                    test_file.write(str(cell_list[0]) + ' | ' + str(cell_list[1]) + ' | ' + str(cell_list[2]) + ' | ' + str(
                        cell_list[3]) + str(cell_list[4]) + ' | ' + str(cell_list[5]) + ' | ' + str(cell_list[6]) + ' | ' + str(
                        cell_list[7]) + str(cell_list[8]) + ' | ' + str(cell_list[9]) + ' | ' + str(cell_list[10]) + ' | ' + str(
                        cell_list[11]) + str(cell_list[12]) + str(cell_list[13]) + ' | ' + str(cell_list[14]) + ' | ' + str(cell_list[15]) + ' | ' + str(
                        cell_list[16]) + '\n')

    return res_list



def get_oss_data_zte4g():
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_zte4g)
    result = cursor.fetchall()
    with open("../../../GET_DATA_FROM_OSS_ZTE_4G_CHECK.txt", "w") as test_file:
        for row in result:
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '\n')
            for item in row:
                test_file.write(str(item) + '\t')
    return result
    cursor.close()


def get_oss_data_NSN4G():
    conn_db = pymysql.connect(host=config.host_mysql_v, user=config.user_mysql_v, password=config.password_mysql_v,
                              db=config.db_mysql_v)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_NSN4g_Voytenko_db)
    result = cursor.fetchall()
    print(result)
    with open(config.path_test4, "w") as test_file:
        for row in result:
           test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '|' + str(row[4]) + '\n')

    return result
    cursor.close()
    print("GOT OSS DATA NSN 4G")

def get_oss_bts_data_NSN4g():
    conn_db = pymysql.connect(host=config.host_mysql_v, user=config.user_mysql_v, password=config.password_mysql_v,
                              db=config.db_mysql_v)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_nsn4g_btsdata_Voytenko_db)
    result = cursor.fetchall()
    print(result)
    with open(config.path_test5, "w") as test_file:
        for row in result:
           test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + '\n')
    return result
    cursor.close()
    print("GOT BTS NSN 4G")

def concat_iot_cells_with_invalid_antennas(oss_list, rpdb_list):
    concatenated_result = list()
    print("Start iot add")

    temp_cell_name_iot = ''
    for iot_cell in range(len(oss_list)):

        if 'IOT' in oss_list[iot_cell][3]:
            for rpdb_cell in range(len(rpdb_list)):
                if len(str(rpdb_list[rpdb_cell][0])) == 9:
                    if str(oss_list[iot_cell][1]) == str(rpdb_list[rpdb_cell][0])[:6] and str(oss_list[iot_cell][3]) != temp_cell_name_iot:
                        temp_cell_name_iot = str(oss_list[iot_cell][3])
                        concatenated_result.append(oss_list[iot_cell] + rpdb_list[rpdb_cell])



    with open(config.path_test6, "w") as test_file:
        for row in concatenated_result:
            test_file.write(str(row) + '\n')

    return concatenated_result
    print("DATA NSN IOT IS CONCATENATED")

def write_lte_data_to_antennas_file_huawei(concatenated_oss_rpdb_data):
    tech = 'LTE'

    with open(config.local_antennas_file_lte, "a") as antennas_file:
        for row in range(len(concatenated_oss_rpdb_data)):
            antenna_profile_from_rpdb_hua = concatenated_oss_rpdb_data[row][5]
            if '/' in antenna_profile_from_rpdb_hua and '.' in antenna_profile_from_rpdb_hua:
                antenna_profile_from_rpdb_hua = antenna_profile_from_rpdb_hua.replace('/', '-')
                antenna_profile_from_rpdb_hua = antenna_profile_from_rpdb_hua.replace('.', '-')
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
    tdd = 'LT_' #ToDo add the check of TDD or FDD
    fdd= 'LF_'
    with open(config.local_antennas_file_lte, "a") as antennas_file:
        for row in range(len(concatenated_oss_rpdb_data)):

            antenna_profile_from_rpdb_zte = str(concatenated_oss_rpdb_data[row][9])
            if '/' in antenna_profile_from_rpdb_zte and '.' in antenna_profile_from_rpdb_zte:
                antenna_profile_from_rpdb_zte = antenna_profile_from_rpdb_zte.replace('/', '-')
                antenna_profile_from_rpdb_zte = antenna_profile_from_rpdb_zte.replace('.', '-')
            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_zte + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_zte = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_zte}"

            antennas_file.write(f"{tech}\t"
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"
                                f"{fdd}{str(concatenated_oss_rpdb_data[row][6])}\t" #enbName
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t"
                                f"{str(concatenated_oss_rpdb_data[row][10])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][11])}\t"  # coordinate
                                f"{fdd}{str(concatenated_oss_rpdb_data[row][5])}\t" # cell  name
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{fdd}{str(concatenated_oss_rpdb_data[row][5]) + '/' + '1'}\t"
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


def  write_lte_data_to_antennas_file_NSN(concatenated_oss_rpdb_data):
    print("START ADD NSN 4G DATA TO ANTENNA FILE")
    tech = 'LTE'
    head = "Technology	RNC Name	RNC Id	NodeB Name	NodeB Id	ENB ID	NodeB Longitude	NodeB Latitude	Sector Name	Active	Noise Figure	AntennaID	Antenna Model	Sector Keywords	Antenna Longitude	Antenna Latitude	Height	Mechanical DownTilt	Azimuth	Downlink Loss	Uplink Loss	RTT fix A Coefficient	RTT fix B Coefficient	RET ID	In Building	Cable Lengths(Calculated)	Sector Height Level(Calculated)"

    with open(config.local_antennas_file_lte, "w") as antennas_file:
        antennas_file.write(head)
        antennas_file.write('\n')
        for row in range(len(concatenated_oss_rpdb_data)):

            antenna_profile_from_rpdb_nsn = concatenated_oss_rpdb_data[row][6]
            if '/' in antenna_profile_from_rpdb_nsn and '.' in antenna_profile_from_rpdb_nsn:
                antenna_profile_from_rpdb_nsn = antenna_profile_from_rpdb_nsn.replace('/', '-')
                antenna_profile_from_rpdb_nsn = antenna_profile_from_rpdb_nsn.replace('.', '-')
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
                                f"{str(concatenated_oss_rpdb_data[row][3])}\t" #cellname
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][3]) + '/' + '1'}\t"
                                f"{antenna_profile_from_rpdb_nsn}\t"  # antenna directory
                                f"{'Nokia'}\t" #sector keyword
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][8])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][12])}\t" # height
                                f"{'0.0'}\t"
                                f"{lte_util.check_azimuth_value(concatenated_oss_rpdb_data[row][13])}\t" # azimuth
                                f"{str(concatenated_oss_rpdb_data[row][14])}\t" # mech tilt
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][16])}\t"
                                f"\t"
                                f"{'Overground'}\n")

    print("The Data has been added to Antenna file")


