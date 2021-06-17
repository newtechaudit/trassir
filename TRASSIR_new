
# coding: utf-8

# In[1]:


import requests
import urllib, urllib3, re, ssl
import os
import ast, json, io, time
import PIL.Image as Image
import pandas as pd
import sys, traceback, shutil

from datetime import datetime, timedelta


# In[2]:

back_days = 1


servers = pd.read_excel(r'/home/user/Рабочий стол/скрипты/TSV/server.xlsx')


#стартовая временная метка для подсчета продолжительности выполнения скрипта
start = time.time()

start_int = 28800 #начальная граница интервала 8:00 a.m.
start_time = time.strftime('%H:%M:%S',time.gmtime(start_int))

finish_int = 72000  #конечная граница интервала 8:00 p.m.
finish_time = datetime.strptime(time.strftime('%H:%M:%S',time.gmtime(finish_int)),'%H:%M:%S')

#для формирования папки с выгруженными скриншотами за определенное число
start_moment = str(datetime.now().date() - timedelta(days=back_days)).replace("-","") + 'T'

catch_time = datetime.strptime(start_time,'%H:%M:%S')


delta_value = 30


# In[3]:


def Get_screenshot(serv_ip, serv_name, chnl_list, catch_time, delta_value, str_dict, session):
    
    start_moment = str(datetime.now().date() - timedelta(days=back_days)).replace("-","") + 'T'
    
        
    
    try:
        os.mkdir("/home/balandin-av/Рабочий стол/скрипты/TSV/" + start_moment + '/' + str(serv_name))
    except:
        print('Folder has already created!', traceback.format_exc())

    
    for f in json.loads(str_dict)['channels']:
        
        
        catch_time = datetime.strptime(start_time,'%H:%M:%S')
        
        print('channel s name ', f['name'])
        
        
        
        try:
            if chnl_list.count(f['name']) > 0:
                try:
                    os.mkdir("/home/user/Рабочий стол/скрипты/TSV/" + start_moment + '/' + str(serv_name) + '/' + f['name'].replace('/','-'))
                except:
                    print('Folder has already created!', traceback.format_exc())

                while catch_time <= finish_time:
                    
                    start_cycle = time.monotonic()
                    
                    catch_moment = str(datetime.now().date() - timedelta(days=back_days)).replace("-","") + 'T' + str(catch_time.time()).replace(":","")

                    print('ID канала ',f['guid'],"Имя канала ",f['name'])
                    rq = "https://" + serv_ip + ":8080/screenshot/" + f['guid'] + "?timestamp=" + catch_moment + "&sid=" + session
                    print('Запрос скриншота ',rq)

                    scr = urllib.request.urlopen(rq,context=ssl._create_unverified_context()).read()
                    img = Image.open(io.BytesIO(scr))

                    img_path = "/home/user/Рабочий стол/скрипты/TSV/"  + start_moment + '/' + str(serv_name) + '/' + f['name'].replace('/','-') + '/' + serv_ip + ' ' + catch_moment + ' ' + f['name'].replace('/','-') + "img.jpeg"
                    print(img_path)
                    img.save(img_path)
                    
                    finish_cycle = time.monotonic()
                    cycle = round((finish_cycle - start_cycle)/60,2)
                    
                    

                    with open('/home/user/Рабочий стол/скрипты/TSV/scr_success.txt','a',encoding='utf-8') as outfile:
                        outfile.write(str(serv_name) + '~' + serv_ip + '~'+ catch_moment + '~scr_success~cycle_time ' + str(cycle))
                        outfile.write('\n')
                    outfile.close()

                    catch_time = catch_time + timedelta(minutes=delta_value)
                    print(catch_time.time())
        except:
            print('failure of screenshots downloading')
            print(traceback.format_exc())

            with open('/home/user/Рабочий стол/скрипты/TSV/scr_failure.txt','a',encoding='utf-8') as outfile:
                outfile.write(str(serv_name) + '~' + serv_ip + '~' + catch_moment + '~scr_failure ' + traceback.format_exc())
                outfile.write('\n')
                outfile.close()

            if traceback.format_exc().count('403'):
                break
        
        
                    
    return print('finish')


# In[6]:


def main():
    try:
        os.mkdir("/home/user/Рабочий стол/скрипты/TSV/" + start_moment)
    except:
        print('Folder has already created!')
        
    bgn = time.monotonic()
    
    with open('/home/user/Рабочий стол/скрипты/TSV/scr_failure.txt','a',encoding='utf-8') as outfile:
        outfile.write('Новая сессия ~' + str(time.ctime()))
        outfile.write('\n')
        outfile.close()
                
    with open('/home/user/Рабочий стол/скрипты/TSV/scr_success.txt','a',encoding='utf-8') as outfile:
        outfile.write('Новая сессия ~' + str(time.ctime()))
        outfile.write('\n')
    outfile.close()
    
    print('requests ',requests.__version__)
    print('urllib3 ',urllib3.__version__)
    print('re ',re.__version__)

    success = 0
    failure = 0

    for idx, inf in servers[:].iterrows():
        print('порядковый номер',idx)
        serv_ip = inf[0]
        serv_name = inf[1]
        chnl_list = inf[2].split(",")
        rqst_ss = 'https://' + serv_ip + ':8080/login?username=uvk&password=uvk'
        print('запрос сессии',rqst_ss)
        try:
            ss = urllib.request.urlopen(rqst_ss,context=ssl._create_unverified_context()).read()
            print(ss)
            ss = json.loads(ss)
            rqst_res = ss['success']
            session = ss['sid']
            print('результат запроса сессии ',rqst_res)
            print('сессия ',session)
            if rqst_res == 0:
                print(serv_ip,' ', serv_name, 'rqst_ss failure')
            else:
                rqst_chnl = "https://" + serv_ip + ":8080/channels/?sid=" + session
                channels = urllib.request.urlopen(rqst_chnl,context=ssl._create_unverified_context()).read()
                channels_str = channels.decode("UTF-8")+"}"
                str_dict = channels_str.split(",\n\t\"remote_channels\"")[0]+"}"
                print('ip ',serv_ip)
                print('Время захвата кадра', catch_time.time())
                print('интервал между кадрами, мин ',delta_value)
                Get_screenshot(serv_ip,serv_name, chnl_list, catch_time,delta_value,str_dict,session)
                success += 1
        except:
            print(serv_ip,' ', serv_name, ' session failure')
            print(traceback.format_exc())
            failure += 1
        
    end = time.monotonic()
    span = round((end - bgn)/60,2)
    
    with open('/home/user/Рабочий стол/скрипты/TSV/total.txt','a',encoding='utf-8') as outfile:
        outfile.write('success: ' + str(success) + '~' + 'failure: ' + str(failure) + '~' + 'total time: ' + str(span))
        outfile.write('\n')
    outfile.close()


if __name__ == '__main__':
    main()

