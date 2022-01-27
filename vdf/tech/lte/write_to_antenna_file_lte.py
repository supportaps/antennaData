import config
from read_directory import read_antenna_directory
from vdf.tech.lte import lte_util


def write_lte_data_to_antennas_file_NSN_test(concatenated_oss_rpdb_data):

    print("START ADD NSN 4G DATA TO ANTENNA FILE")
    tech = 'LTE'
    head = "Technology	RNC Name	RNC Id	NodeB Name	NodeB Id	ENB ID	NodeB Longitude	NodeB Latitude	Sector Name	Active	Noise Figure	AntennaID	Antenna Model	Sector Keywords	Antenna Longitude	Antenna Latitude	Height	Mechanical DownTilt	Azimuth	Downlink Loss	Uplink Loss	RTT fix A Coefficient	RTT fix B Coefficient	RET ID	In Building	Cable Lengths(Calculated)	Sector Height Level(Calculated)"


    with open(config.local_antennas_file_lte_test, "a") as antennas_file:
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
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t" #sitename test for id
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

def write_lte_data_to_antennas_file_NSN_IOT_TEST(concatenated_oss_rpdb_data):
    print("START ADD NSN 4G DATA TO ANTENNA FILE")
    tech = 'LTE'
    head = "Technology	RNC Name	RNC Id	NodeB Name	NodeB Id	ENB ID	NodeB Longitude	NodeB Latitude	Sector Name	Active	Noise Figure	AntennaID	Antenna Model	Sector Keywords	Antenna Longitude	Antenna Latitude	Height	Mechanical DownTilt	Azimuth	Downlink Loss	Uplink Loss	RTT fix A Coefficient	RTT fix B Coefficient	RET ID	In Building	Cable Lengths(Calculated)	Sector Height Level(Calculated)"

    with open(config.local_antennas_file_lte_test, "a") as antennas_file:
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
                                f"{str(concatenated_oss_rpdb_data[row][0])}\t" #sitename test for id
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
                                f"{lte_util.check_azimuth_value(concatenated_oss_rpdb_data[row][13])}\t" # azimuth
                                f"{str(concatenated_oss_rpdb_data[row][14])}\t" # mech tilt
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][16])}\t"
                                f"\t"
                                f"{'Overground'}\n")
    print("The Data has been added to Antenna file TEST")

def write_lte_data_to_antennas_file_NSN_IOT(concatenated_oss_rpdb_data):
    print("START ADD NSN 4G DATA TO ANTENNA FILE")
    tech = 'LTE'

    with open(config.local_antennas_file_lte, "a") as antennas_file:

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
                                f"{str(concatenated_oss_rpdb_data[row][0])}\t" #sitename test for id
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t" #id
                                f"{str(concatenated_oss_rpdb_data[row][1])}\t" #id
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][8])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][3])}\t" #cell name
                                f"{'true'}\t"
                                f"{'5.0'}\t"
                                f"{str(concatenated_oss_rpdb_data[row][3]) + '/' + '1'}\t"
                                f"{antenna_profile_from_rpdb_nsn}\t"  # antenna directory
                                f"{'NSN IOT'}\t" #sector keyword
                                f"{str(concatenated_oss_rpdb_data[row][7])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][8])}\t"  # coordinate
                                f"{str(concatenated_oss_rpdb_data[row][12])}\t" # height
                                f"{'0.0'}\t"
                                f"{lte_util.check_azimuth_value(str(concatenated_oss_rpdb_data[row][3]))}\t" # azimuth
                                f"{str(concatenated_oss_rpdb_data[row][14])}\t" # mech tilt
                                f"{3.0}\t"
                                f"{0.0}\t"
                                f"{0.0}\t"
                                f"\t"
                                f"{str(concatenated_oss_rpdb_data[row][16])}\t"
                                f"\t"
                                f"{'Overground'}\n")
    print("The Data has been added to Antenna file")

def write_lte_data_to_antennas_file_NSN_invalid_antennas_TEST(invalid_antenna_data):


    invalid_antenna_data.to_csv(config.local_antennas_file_lte, header=None, index=None,
                               sep='\t', mode='a')


