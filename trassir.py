import os
import ast
import json, io, time
import PIL.Image as Image
import pandas as pd
import sys
from datetime import datetime, timedelta, date
import warnings
import asyncio
import aiohttp
from pathlib import Path
from tqdm.asyncio import tqdm
warnings.filterwarnings('ignore')

# root = '/home/user/Рабочий стол/скрипты/TSV/'
root = ''
servers = pd.read_excel(os.path.join(root, 'server.xlsx'))

# SETTINGS
# set times variables for screenshots query
# start moment 08:00:00

# стартовая временная метка для подсчета продолжительности выполнения скрипта
start = time.time()

start_int = 28800 # 8:00 a.m.
# start_int = 32400 # 9:00 a.m.
start_time = time.strftime('%H:%M:%S', time.gmtime(start_int))

# finish moment 20:00:00
finish_int = 72000  # 8:00 p.m.
# finish_int = 30600  # 8:30 a.m.
# finish_int = 36000   # 10:00 a.m.
finish_time = datetime.strptime(time.strftime('%H:%M:%S', time.gmtime(finish_int)), '%H:%M:%S')

catch_time = datetime.strptime(start_time, '%H:%M:%S')
delta_value = 30
catch_time_list = [catch_time + i * timedelta(minutes=delta_value) for i in range((finish_int - start_int) // (delta_value * 60))]


def main(start_moment):
    try:
        os.mkdir(os.path.join(root, start_moment))
    except:
        print('Folder has already created!')
        
    # записываем в логи информацию о начале новой сессии
    with open(os.path.join(root, 'logs', 'scr_failure.txt'),'a',encoding='utf-8') as outfile:
        outfile.write('Новая сессия ~' + str(time.ctime()) + '\n')
                
    with open(os.path.join(root, 'logs', 'scr_success.txt'),'a',encoding='utf-8') as outfile:
        outfile.write('Новая сессия ~' + str(time.ctime()) + '\n')
    
    # print(servers)
    
    loop = asyncio.get_event_loop()
    
    future = asyncio.ensure_future(work_with_server(servers, start_moment))
    loop.run_until_complete(future)
    
    
async def bound_fetch(sem, url, session, serv_ip, serv_name, start_moment):
    """
    Функция подключает Семафор
    Параметры:
        sem - aiohttp.Semaphore
        url - url-адрес текущего сервера
        session - сессия aiohttp
        serv_ip - ip-адрес текущего сервера
        serv_name - имя сервера (номер ГОСБа-номер ВСП)
    Возвращаемого значения нет
    """
    async with sem:
        await fetch(url, session, serv_ip, serv_name, start_moment)
        
        
async def fetch(url, session, serv_ip, serv_name, start_moment):
    """
    Здесь и происходит главное: Получение id сессии, получение списка каналов и выгрузка скриншотов по каждому каналу
    Параметры:
        url - url-адрес сервера, с которым сейчас работаем
        session - сессия aiohttp
        serv_ip - ip-адрес текущего сервера
        serv_name - имя сервера (номер ГОСБа-номер ВСП)
        
    Возвращаемого значения нет
    """
    try:
        async with session.get(url, verify_ssl=False) as response:
            content_text = await response.json()
            sid = content_text['sid']
            
        rqst_chnl = "https://" + serv_ip + ":8080/channels/?sid=" + sid
    
        async with session.get(rqst_chnl, verify_ssl=False) as jopa:
            resp = await jopa.read()
            guids = []
            if resp:
                guids, names = channels_handler(resp)
        
        catch_times = [start_moment + str(catch_time.time()).replace(":","") for catch_time in catch_time_list] 
        for guid, name in zip(tqdm(guids, desc='guids_bar', leave=False), names):
            Path(os.path.join(root, start_moment, serv_name, str(guid))).mkdir(exist_ok=True)
            for catch_timee in tqdm(catch_times, desc='screenshots', leave=False):
                # print('screen request begin, ' + serv_ip)
                async with session.get("https://" + \
                                  serv_ip + \
                                  ":8080/screenshot/" + \
                                  guid + \
                                  "?timestamp=" + \
                                  catch_timee + \
                                  '&sid=' + \
                                  sid, verify_ssl=False) as screen:
                    scr = screen.content_type
                    # print('screen request end, ' + serv_ip)
                    
                    # убеждаемся, что нам прилетел именно скриншот
                    if scr == 'image/jpeg':
                        img = await screen.read()
                        img_path = os.path.join(root, start_moment, serv_name, str(name), str(catch_timee) + '_' + "img.jpeg")
                        save_image(img, img_path)
                        
    except json.decoder.JSONDecodeError:
        save_error('json_decode_error.txt', serv_ip)
            
    except aiohttp.client_exceptions.ClientConnectorError:
        save_error('connection_failed.txt', serv_ip)
    
    except FileNotFoundError:
        return 'error'
    
    except:
        save_error('other_errors.txt', serv_ip + '~' + str(sys.exc_info()))
        
        
def save_error(file_name, error_text):
    """
    Функция сохраяет текст ошибки в определенный файл
    Параметры:
        file_name - имя файла, куда нужно сохранить ошибку
        error_text - текст ошибки
    Возвращаемое значение:
        нет
    """
    with open(os.path.join(root, file_name), 'a') as hfile:
        hfile.write(error_text + '\n')
    
    
async def work_with_server(servers, start_moment):
    """
    Функция формирует список сопрограмм по каждому серверу
    Параметры:
        servers - список серверов
    Возвращаемого значения нет
    """
    sem = asyncio.Semaphore(10)
    tasks = []
    
    async with aiohttp.ClientSession() as session:        
        for idx, inf in servers[:].iterrows():
            serv_ip = inf[0]
            serv_name = inf[1]
            Path(os.path.join(root, start_moment, serv_name)).mkdir(exist_ok=True)
            serv_url = 'https://' + serv_ip + ':8080/login?username=uvk&password=uvk'
            task = asyncio.ensure_future(bound_fetch(sem, serv_url, session, serv_ip, serv_name, start_moment))
            tasks.append(task)
          
        # await asyncio.gather(*tasks)
        results = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='servers_bar')]
        

def channels_handler(channel_response):
    """
    Функция обрабатывает ответ на запрос списка каналов и выдаёт список guid этих каналов
    Параметры:
        channel_response - ответ сервера на запрос списка каналов
    Возвращаемое значение:
        список guid каждого канала
    """
    channels = channel_response.decode('utf-8')
    channels = channels[:channels.find('/*')]
    channels_dict = ast.literal_eval(channels)
    channels_list = channels_dict['channels']
    names = [channel['name'] for channel in channels_list]
    guids = [channel['guid'] for channel in channels_list]
    return guids, names


def save_image(im_byte, img_path):
    """
    Функция сохраняет картинку
    Параметры:
        im_byte - картинка в байтовом виде
        img_path - путь, по которому надо сохранить картинку
    Возвращаемого значения нет
    """
    img = Image.open(io.BytesIO(im_byte))
    img.save(img_path)


if __name__ == '__main__':
    start_date = ''
    
    if len(sys.argv) == 2:
        start_date = sys.argv[1]
    else:
        # если не указана конкретная дата, берем вчерашний день
        start_date = (date.today() - timedelta(days=1)).strftime('%Y%m%d') + 'T'
        
    print(start_date)
    main(start_date)

