import ftplib
import re
import asyncio
import os
import logging
import argparse
from datetime import datetime, date


async def uncompress(file_name):
    # os.system(f'uncompress {file_name}')
    await asyncio.get_running_loop().run_in_executor(None, os.system, f'uncompress {file_name} -f')


async def download_file(file_name, item, ftps):
    with open(file_name, 'wb') as file:
        # ftps.retrbinary("RETR " + item, file.write)
        await asyncio.get_running_loop().run_in_executor(None, ftps.retrbinary, f'RETR {item}', file.write)


async def save_file(file_name, item, week):
    logger.info(f'Загрузка файла {item}...')
    ftps = start_ftps(week)
    await download_file(file_name, item, ftps)
    ftps.quit()
    logger.info(f'Файл {item} успешно загружен.\nРаспаковка...')
    await uncompress(file_name)
    logger.info(f'Файл {item} успешно распакован')
    
def start_ftps(week):
    try:
        ftps = ftplib.FTP_TLS('gdc.cddis.eosdis.nasa.gov')
        ftps.login('anonymous', '')
        ftps.prot_p()
        ftps.cwd(f'/pub/gps/products/{week}')
        return ftps
    except ftplib.all_errors as e:
        print(f'Ошибка соединения: {str(e)}')
        exit()

def remove_empty_folders(folder):
    for root, dirs, _ in os.walk(folder, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)

async def download_all_files(weeks, org_sources, formats):
    tasks = []
    n_load_files = 0
    for week in weeks:
        ftps = start_ftps(week)
        ftp_week_files = ftps.nlst()
        ftps.quit()
        for org_source in org_sources:
            for format in formats:
                path = f'{format}/{org_source}'
                pattern = f'{org_source}{week}[0-6]\.{format}\.Z'
                if not os.path.exists(path):
                    os.makedirs(path)
                local_files = [f'{x}.Z' for x in os.listdir(path)]
                ftp_week_files_required = [x for x in ftp_week_files if re.match(pattern, x)]
                load_files = [x for x in ftp_week_files_required if x not in local_files]
                n_load_files += len(load_files)
                logger.debug(f'[{week} | {org_source} | {format}] Файлов для загрузки: {len(load_files)}')
                for item in load_files:
                    file_name = f'{format}/{org_source}/{item}'
                    task = asyncio.create_task(
                        save_file(file_name, item, week))
                    tasks.append(task)
    logger.info(f'Файлов для загрузки: {n_load_files}')
    await asyncio.gather(*tasks)
    remove_empty_folders('.')
    
def get_gps_week(date_str):
    start_date = datetime(1980, 1, 6)
    target_date = datetime.strptime(date_str, '%d.%m.%Y')
    gps_week = (target_date - start_date).days // 7
    return gps_week

logger = None

if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    
    parser = argparse.ArgumentParser(description='Описание программы:\n Это консольное приложение для загрузки данных с серверов CDDIS.NASA.' + 
                                    'Программа обеспечивает удобный доступ получения геодезических данных, сгенерированных и поддреживаемых NASA, для анализа и дальнейшего использования')
    parser.add_argument('--weeks', nargs='?', type=str, default='None',  help='номера недели. По умолчанию: "2175 2176"')
    parser.add_argument('--sources', nargs='?', type=str, default='None',  help='источники. По умолчанию: "cod igs"')
    parser.add_argument('--formats', nargs='?', type=str, default='None',  help='форматы файлов. По умолчанию: "eph clk sp3"')
    parser.add_argument('--getweek', nargs='?', type=str, default='None', help='получить неделю по дате. Формат даты: dd.mm.yyyy')
    

    args = parser.parse_args()
    logger.info(f'Полученные недели: ' + args.weeks)
    logger.info(f'Полученные источники ' + args.sources)
    logger.info(f'Полученный день ' + args.getweek)
    # weeks = ['2175','2176']
    # org_sources = ['cod', 'igs']
    # formats = ['eph', 'clk', 'sp3']
    weeks = []
    org_sources = []
    formats = []
    
    # if not check_ftp_connection('gdc.cddis.eosdis.nasa.gov', 'anonymous', ''):
    #     print('Не удалось установить соединение с сервером')
    #     exit()

    if args.weeks != 'None' or args.sources != 'None' or args.formats != 'None':
        if args.weeks == 'None':
            logger.info(f'Недели не выбраны. Используются параметры по умолчанию: "2175 2176"')
            weeks = ['2175','2176']
        else: 
            weeks = args.weeks.split(' ')
        if args.sources == 'None':
            logger.info(f'Источники не выбраны. Используются параметры по умолчанию: "cod igs"')
            org_sources = ['cod', 'igs']
        else:
            org_sources = args.sources.split(' ')
        if args.formats == 'None':
            logger.info(f'Форматы не выбраны. Используются параметры по умолчанию: "eph clk sp3"')
            formats = ['eph', 'clk', 'sp3']
        else:
            formats = args.formats.split(' ')
        loop = asyncio.new_event_loop()
        loop.run_until_complete(download_all_files(weeks, org_sources, formats))
    else:
        if args.getweek != 'None':
            gps_week = get_gps_week(args.getweek)
            print(gps_week)
        else:
            print('Параметры не выбраны. Используйте при запуске: --help')
