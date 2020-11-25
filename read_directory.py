import os
import config


def read_antenna_directory():
    antenna_profile_list =  []
    for dir in os.listdir(config.antennas_file_path):

        profile_antenna_path = config.profile_antenna_path_mentor + rf"\{dir}"
        for profile in os.listdir(profile_antenna_path):
            res = (dir, profile)
            antenna_profile_list.append(res)

    return antenna_profile_list




if __name__ == '__main__':
    read_antenna_directory()
