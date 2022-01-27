import config
import pandas as pd

def get_invalid_lte_antennas_data():


    invalid_antennas_fr = pd.read_table(config.invalid_antennas_path, sep='\t')
    invalid_antennas_fr = invalid_antennas_fr[invalid_antennas_fr['Antenna Longitude'] == 0]
    invalid_antennas_fr = invalid_antennas_fr[invalid_antennas_fr['Technology'] == 'LTE']


    azimuth = invalid_antennas_fr['Sector Name'].apply(lambda s: s[12:15])

    invalid_antennas_fr['Antenna Longitude'] = invalid_antennas_fr['NodeB Longitude']
    invalid_antennas_fr['Antenna Latitude'] = invalid_antennas_fr['NodeB Latitude']
    invalid_antennas_fr['Azimuth'] = azimuth
    invalid_antennas_fr['Height'] = 20
    invalid_antennas_fr['Mechanical DownTilt'] = 0
    invalid_antennas_fr['Sector Keywords'] = 'MISMATCHED, PHYSICAL DATA IS INCORRECT'
    invalid_antennas_fr['ENB ID'] = invalid_antennas_fr['ENB ID'].apply(lambda x: int(x))
    invalid_antennas_fr.to_csv(r'D:\Jaroslaw\py_proj\antennaData\INVALID_ANTENNAS.txt', header=None, index=None, sep='\t', mode='w')


    return invalid_antennas_fr