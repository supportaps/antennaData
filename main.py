from cProfile import Profile
from pstats import Stats

import pymssql
import pymysql

import config
import gsm_coordinates_file
from read_directory import read_antenna_directory, copy_antenna_file_to_remote
import lte_antenna_file


def get_data_from_wbts_nsn():
    result_list_wbts = []
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_wbts_nsn)
    result_set_wbts = cursor.fetchall()
    with open("GET_DATA_FROM_OSS_NSN_3G_WBTS_CHECK.txt", "w") as test_file:
        for row in result_set_wbts:
            result_list_wbts.append(row)
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + '\n')
    cursor.close()

    return result_list_wbts


def get_data_from_wcel_nsn():
    result_list_wcel = []
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_wcel_nsn)
    result_set_wcel = cursor.fetchall()
    with open("GET_DATA_FROM_OSS_NSN_3G_WCEL_CHECK.txt", "w") as test_file:
        for row in result_set_wcel:
            result_list_wcel.append(row)
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3]) + str(row[4]) + ' | ' + str(row[5]) + '\n')
    cursor.close()

    return result_set_wcel


def get_data_from_hua_ucell():
    result_list_ucell = []
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_ucell_hua)
    result_set_ucell = cursor.fetchall()
    for row in result_set_ucell:
        result_list_ucell.append(row)
    cursor.close()

    return result_list_ucell


def get_data_from_hua_nodeb():
    result_list_node = []
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_nodeb_hua)
    result_set_node = cursor.fetchall()
    for row in result_set_node:
        result_list_node.append(row)
    cursor.close()

    return result_list_node


def add_bts_name_to_list_nsn(wcel_list, wbts_list):
    result_list_mixed = []

    # print(wbts_list)
    # print(wcel_list)
    for wcel in range(len(wcel_list)):
        # for column in range(len(wcel_list[wcel])):
        # print('wcel : ',wcel_list[wcel] )
        for wbts in range(len(wbts_list)):
            # print(wbts_list[wbts])
            if wcel_list[wcel][2] == wbts_list[wbts][1] and wcel_list[wcel][5] == wbts_list[wbts][0]:
                combined_tuple = wcel_list[wcel] + wbts_list[wbts]
                result_list_mixed.append(combined_tuple)

    return result_list_mixed


def add_bts_name_to_list_hua(ucell_list, nodeb_list):
    result_list_mixed = []
    # print(ucell_list)
    # print(nodeb_list)

    for ucel in range(len(ucell_list)):
        for nodeb in range(len(nodeb_list)):
            if ucell_list[ucel][3] == nodeb_list[nodeb][4]:
                combined_tuple = ucell_list[ucel] + nodeb_list[nodeb]
                result_list_mixed.append(combined_tuple)

    # print(result_list_mixed)
    return result_list_mixed


def get_data_from_rpdb():
    conn_db = pymssql.connect(host=config.host_mssql, user=config.user_mssql, password=config.password_mssql,
                              database=config.db_mssql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_rpdb_mentor)
    result_list_rpdb = []
    result_set = cursor.fetchall()
    with open("GET_DATA_FROM_RPDB.txt", "w") as test_file:
        for row in result_set:
            print("----->>>>>>", row[1][-2:])
            if row[1][-2:] != '_D' and row[1][-2:] != '_G':
                print("RESULT LIST: ", row)

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



def combined_list_from_rpdb_and_oss_to_antennas_file(oss_list, rpdb_list):
    result_list_rpdb_oss_data = []
    # print(oss_list)
    for oss_cell in range(len(oss_list)):
        for rpdb_cell in range(len(rpdb_list)):
            if oss_list[oss_cell][0] == rpdb_list[rpdb_cell][2] and oss_list[oss_cell][1] == rpdb_list[rpdb_cell][3]:
                # print(rpdb_list[rpdb_cell], "------>>>>>", oss_list[oss_cell])
                combined_tuple = rpdb_list[rpdb_cell] + oss_list[oss_cell]
                result_list_rpdb_oss_data.append(combined_tuple)

    return result_list_rpdb_oss_data


def combined_list_from_rpdb_and_oss_to_antennas_file_huawei(oss_list, rpdb_list):
    result_list_rpdb_oss_data = []
    for oss_cell in range(len(oss_list)):
        for rpdb_cell in range(len(rpdb_list)):
            lac = oss_list[oss_cell][0][2:]
            lac_dec = int(lac, 16)
            # print("LAC and LAC_DEC: ", lac, lac_dec)
            if str(lac_dec) == str(rpdb_list[rpdb_cell][2]) and oss_list[oss_cell][1] == rpdb_list[rpdb_cell][3]:
                # print(oss_list[oss_cell][1] ,rpdb_list[rpdb_cell][3], "------>>>>>", str(lac_dec) ,rpdb_list[rpdb_cell][2])
                combined_tuple = rpdb_list[rpdb_cell] + oss_list[oss_cell]
                result_list_rpdb_oss_data.append(combined_tuple)

    return result_list_rpdb_oss_data


def combined_list_from_rpdb_and_oss_to_antennas_file_zte(oss_list, rpdb_list):
    concatenated_result = list()
    print(oss_list[0], rpdb_list[0])

    for oss_item in range(len(oss_list)):
        item_oss1 = int(oss_list[oss_item][3].replace(" ", ''))  # LAC
        item_oss2 = oss_list[oss_item][1]  # CI
        item_oss3 = oss_list[oss_item][0]  # RNC ID
        if item_oss3 is None:
            item_oss3 = 0
        # print('TYPE_ITENM',type(item_oss1),type(item_oss2))
        low = 0
        found = False
        high = len(rpdb_list) - 1

        while low <= high and not found:
            mid = (low + high) // 2
            guess1 = rpdb_list[mid][2]  # LAC
            guess2 = rpdb_list[mid][3]  # CI
            guess3 = rpdb_list[mid][0]  # RNC ID
            if guess3 is None:
                guess3 = 0
            # print('TYPE_GUESS',type(guess1),type(guess2))
            if int(guess1) == int(item_oss1) and int(guess2) == int(item_oss2):
                found = True
                concatenated_result.append(oss_list[oss_item] + rpdb_list[mid])
            elif int(guess2) + int(guess1) + int(guess3) >= int(item_oss2) + int(item_oss1) + int(item_oss3):
                high = mid - 1
            elif int(guess2) + int(guess1) + int(guess3) <= int(item_oss2) + int(item_oss1) + int(item_oss3):
                low = mid + 1
    # print("concatenated result: ", concatenated_result)
    return concatenated_result


def get_oss_data_zte():
    result_list_ucell = []
    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                              db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_zte_3g)
    result_set_ucell = cursor.fetchall()
    with open("GET_DATA_FROM_OSS.txt", "a") as test_file:
        for row in result_set_ucell:
            temp_tuple = (row[0], row[1], row[2], row[3].split('/')[3].replace('ULocationArea=', ''))
            result_list_ucell.append(temp_tuple)
            test_file.write(str(row[0]) + ' | ' + str(row[1]) + ' | ' + str(row[2]) + ' | ' + str(
                row[3].split('/')[3].replace('ULocationArea=', '')) + '\n')
        cursor.close()

    return result_list_ucell


def main():

    wcel_list = get_data_from_wcel_nsn()
    wbts_list = get_data_from_wbts_nsn()
    oss_data_list_nsn = add_bts_name_to_list_nsn(wcel_list, wbts_list)

    res4g_rpdb = lte_antenna_file.get_rpdb_data_4g()

    NSN_concat_result = lte_antenna_file.concat_oss_rpdb_data_binary_search_4g(lte_antenna_file.get_oss_data_NSN4G(),
                                                                               res4g_rpdb)

    print("NOKIA CONCAT RESULT", NSN_concat_result)

    huawei_concat_result = lte_antenna_file.concat_oss_rpdb_data_binary_search_4g(
        lte_antenna_file.get_oss_data_huawei4g(), res4g_rpdb)




    #print("HUAWEI RESULT", huawei_concat_result)

    zte_concat_result = lte_antenna_file.concat_oss_rpdb_data_binary_search_4g(
        lte_antenna_file.get_oss_data_zte4g(), res4g_rpdb)
    #print("zte_concat_result", zte_concat_result)
    print("DONE")




    rpdb_list = get_data_from_rpdb()

    list_of_antenna_file_zte = combined_list_from_rpdb_and_oss_to_antennas_file_zte(get_oss_data_zte(), rpdb_list)

    tech = 'UMTS'
    equipment = 'RNC'
    head = "Technology	RNC Name	RNC Id	NodeB Name	NodeB Id	ENB ID	NodeB Longitude	NodeB Latitude	Sector Name	Active	Noise Figure	AntennaID	Antenna Model	Sector Keywords	Antenna Longitude	Antenna Latitude	Height	Mechanical DownTilt	Azimuth	Downlink Loss	Uplink Loss	RTT fix A Coefficient	RTT fix B Coefficient	RET ID	In Building	Cable Lengths(Calculated)	Sector Height Level(Calculated)"
    antenna_number = 0

    with open("Antennas.txt", "w") as antennas_file:

        antennas_file.write(head)
        antennas_file.write('\n')

        for row in range(len(list_of_antenna_file_zte)):

            # antenna_number_hua = int(list_for_antenna_file_huawei[row][14])
            # antenna_number_hua = str(antenna_number_hua)[-1]
            azimuth = int(list_of_antenna_file_zte[row][14])
            print("AZ", azimuth)
            antenna_profile_from_rpdb_zte = list_of_antenna_file_zte[row][10]
            print("PROF", antenna_profile_from_rpdb_zte)
            nodeBnameZte = str(list_of_antenna_file_zte[row][2][:11])
            print("NodeB", nodeBnameZte)

            cellName = str(list_of_antenna_file_zte[row][2])
            print("cell", cellName)
            rnc_id = str(list_of_antenna_file_zte[row][0])
            print("rnc_id", rnc_id, ' - ', type(rnc_id))

            nodeB_id = str(list_of_antenna_file_zte[row][1])
            nodeB_id = nodeB_id[:4]
            print("nodeb_id", nodeB_id, ' - ', type(nodeB_id))

            if azimuth == 360 or azimuth in range(859, 999):
                azimuth = 0

            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_zte + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_zte = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_zte}"

            antennas_file.write(f"{tech}\t"
                                f"{equipment + rnc_id}\t"
                                f"{rnc_id}\t"
                                f"{nodeBnameZte}\t"
                                f"{str(list_of_antenna_file_zte[row][0]) + '_' + nodeB_id}\t"  # emphesis symbol amount
                                f"{'N/A'}\t"
                                f"{list_of_antenna_file_zte[row][12]}\t"  # coordinate
                                f"{list_of_antenna_file_zte[row][11]}\t"  # coordinate
                                f"{cellName}\t"
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{nodeB_id + '/' + str(azimuth)}\t"
                                f"{antenna_profile_from_rpdb_zte}\t"  # antenna directory
                                f"\t"
                                f"{list_of_antenna_file_zte[row][12]}\t"  # coordinate
                                f"{list_of_antenna_file_zte[row][11]}\t"  # coordinate
                                f"{str(list_of_antenna_file_zte[row][13])}\t"
                                f"{'0.0'}\t"
                                f"{azimuth}\t"
                                f"{'3.0'}\t"
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{'false'}\t"
                                f"\t"
                                f"{'Overground'}\n")


    # print('res4g_rpdb')



    # print(lte_antenna_file.get_rpdb_data_4g().sort(key=lambda tup: tup[3]))




    ucel_list = get_data_from_hua_ucell()
    nodeb_list_hua = get_data_from_hua_nodeb()

    oss_data_list_hua = add_bts_name_to_list_hua(ucel_list, nodeb_list_hua)



    list_for_antenna_file_huawei = combined_list_from_rpdb_and_oss_to_antennas_file_huawei(oss_data_list_hua, rpdb_list)
    # print("LIST COMPLETE________________", list_for_antenna_file_huawei)
    # print("LIST COMPLETE")
    list_for_antenna_file = combined_list_from_rpdb_and_oss_to_antennas_file(oss_data_list_nsn, rpdb_list)

    with open("Antennas.txt", "a") as antennas_file:

        for row in range(len(list_for_antenna_file)):
            # for el in range(len(list_for_antenna_file[row])):

            antenna_number = int(list_for_antenna_file[row][16])
            azimuth = int(list_for_antenna_file[row][10])
            antenna_profile_from_rpdb = list_for_antenna_file[row][6]

            if antenna_number == 1 or antenna_number == 2 or antenna_number == 3:
                antenna_number = 1
            elif antenna_number == 4 or antenna_number == 5 or antenna_number == 6:
                antenna_number = 2
            elif antenna_number == 7 or antenna_number == 8 or antenna_number == 9:
                antenna_number = 3
            elif antenna_number == 10 or antenna_number == 11 or antenna_number == 12:
                antenna_number = 4
            elif antenna_number == 13 or antenna_number == 14 or antenna_number == 15:
                antenna_number = 5
            elif antenna_number == 16 or antenna_number == 17 or antenna_number == 18:
                antenna_number = 6

            if azimuth == 360 or azimuth in range(859, 999):
                azimuth = 0

            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb}"

            antennas_file.write(f"{tech}\t"
                                f"{equipment + str(list_for_antenna_file[row][19])}\t"
                                f"{str(list_for_antenna_file[row][19])}\t"
                                f"{str(list_for_antenna_file[row][22])}\t"
                                f"{str(list_for_antenna_file[row][19]) + '_' + str(list_for_antenna_file[row][20])}\t"
                                f"{'N/A'}\t"
                                f"{list_for_antenna_file[row][8]}\t"  # coordinate
                                f"{list_for_antenna_file[row][7]}\t"  # coordinate
                                f"{list_for_antenna_file[row][17]}\t"
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(list_for_antenna_file[row][20]) + '/' + str(antenna_number)}\t"
                                f"{antenna_profile_from_rpdb}\t"  # antenna directory
                                f"\t"
                                f"{list_for_antenna_file[row][8]}\t"  # coordinate
                                f"{list_for_antenna_file[row][7]}\t"  # coordinate
                                f"{str(list_for_antenna_file[row][9])}\t"
                                f"{'0.0'}\t"
                                f"{azimuth}\t"
                                f"{'3.0'}\t"
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{'false'}\t"
                                f"\t"
                                f"{'Overground'}\n")

    with open("Antennas.txt", "a") as antennas_file:

        for row in range(len(list_for_antenna_file_huawei)):

            # antenna_number_hua = int(list_for_antenna_file_huawei[row][14])
            # antenna_number_hua = str(antenna_number_hua)[-1]
            azimuth = int(list_for_antenna_file_huawei[row][10])
            antenna_profile_from_rpdb_hua = list_for_antenna_file_huawei[row][6]
            nodeBname = str(list_for_antenna_file_huawei[row][22]).replace('\"', '')
            cellName = str(list_for_antenna_file_huawei[row][15]).replace('\"', '')

            if azimuth == 360 or azimuth in range(859, 999):
                azimuth = 0

            antenna_directory_list = read_antenna_directory()
            antenna_profile_name = antenna_profile_from_rpdb_hua + '.txt'
            for profile in range(len(antenna_directory_list)):
                for antenna in range(len(antenna_directory_list[profile])):
                    if antenna_directory_list[profile][antenna] == antenna_profile_name:
                        antenna_profile_from_rpdb_hua = f"{antenna_directory_list[profile][0]}/{antenna_profile_from_rpdb_hua}"

            antennas_file.write(f"{tech}\t"
                                f"{equipment + str(list_for_antenna_file_huawei[row][18])}\t"
                                f"{str(list_for_antenna_file_huawei[row][18])}\t"
                                f"{nodeBname}\t"
                                f"{str(list_for_antenna_file_huawei[row][18]) + '_' + str(list_for_antenna_file_huawei[row][19])}\t"
                                f"{'N/A'}\t"
                                f"{list_for_antenna_file_huawei[row][8]}\t"  # coordinate
                                f"{list_for_antenna_file_huawei[row][7]}\t"  # coordinate
                                f"{cellName}\t"
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(list_for_antenna_file_huawei[row][19]) + '/' + str(azimuth)}\t"
                                f"{antenna_profile_from_rpdb_hua}\t"  # antenna directory
                                f"\t"
                                f"{list_for_antenna_file_huawei[row][8]}\t"  # coordinate
                                f"{list_for_antenna_file_huawei[row][7]}\t"  # coordinate
                                f"{str(list_for_antenna_file_huawei[row][9])}\t"
                                f"{'0.0'}\t"
                                f"{azimuth}\t"
                                f"{'3.0'}\t"
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{'false'}\t"
                                f"\t"
                                f"{'Overground'}\n")

    lte_antenna_file.write_lte_data_to_antennas_file_NSN(NSN_concat_result)
    lte_antenna_file.write_lte_data_to_antennas_file_huawei(huawei_concat_result)
    lte_antenna_file.write_lte_data_to_antennas_file_zte(zte_concat_result)

    copy_antenna_file_to_remote()

    rpdb_2g_data = gsm_coordinates_file.get_data_from_rpdb_2G()
    oss_huawei_2g_data = gsm_coordinates_file.get_oss_data_huawei_2g()
    two_g_huawei_concat_result = gsm_coordinates_file.concat_oss_rpdb_data_binary_search_2g(oss_huawei_2g_data, rpdb_2g_data)
    gsm_coordinates_file.write_gsm_huawei_to_coordinates_file(two_g_huawei_concat_result)



if __name__ == '__main__':
    profiler = Profile()
    profiler.runcall(main)  # put main function to check
    stats = Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats("cumulative")
    stats.print_stats()
