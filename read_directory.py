import os
import config
import shutil


def read_antenna_directory():
    antenna_profile_list =  []
    for dir in os.listdir(config.antennas_file_path):

        profile_antenna_path = config.profile_antenna_path_mentor + rf"\{dir}"
        for profile in os.listdir(profile_antenna_path):
            res = (dir, profile)
            antenna_profile_list.append(res)

    return antenna_profile_list


def copy_antenna_file_to_remote():
    src_path = config.local_antennas_file_path + r'\Antennas.txt'
    dst_path = config.remote_antennas_file_path

    shutil.copy(src_path, dst_path)
    print("The Antenna file has been copied")
    print(src_path, dst_path)
if __name__ == '__main__':
    copy_antenna_file_to_remote()
