import pymysql
import config

def check_azimuth_value(azimuth):
    if isinstance(azimuth, str):
        azimuth = azimuth[12:15]
        if azimuth == '360' or int(azimuth) in range(859, 999):
            azimuth = '0'
        return azimuth
    else:

        if azimuth == 360 or azimuth in range(859, 999):
            azimuth = '0'
        return azimuth


def correct_cell_name(lnbts, lncel):

    conn_db = pymysql.connect(host=config.host_mysql, user=config.user_mysql, password=config.password_mysql,
                                  db=config.db_mysql)
    cursor = conn_db.cursor()
    cursor.execute(config.query_oss_nsn_to_correct_cell_name(lnbts, lncel))
    result = cursor.fetchall()
    print(str(result[0]))
    cell_name = str(result[0])
    cell_name = cell_name.replace('(', '')
    cell_name = cell_name.replace(')', '')
    cell_name = cell_name.replace("'", '')
    cell_name = cell_name.replace(",", '')
    print(cell_name)
    return cell_name


