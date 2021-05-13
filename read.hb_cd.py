# coding: utf-8

import json
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from fractions import Fraction
import matplotlib.pyplot as plt

# 全局变量
start_time = ''
end_time = ''
start_second = 0 
end_second = 0
spn_dict = {}
pgn_dict = {}
json_list = []
hex_spn_dict = {}
isFliterNA = 0

# 1、读取一条HB，把快照参数保存为字典dict{spn:value}
# json它是一种基于文本，独立于语言的轻量级数据交换格式,一般接口传输数据用。
def read_hb_json(fn_json):

    global start_time, end_time, start_second, end_second

    data = json.loads(fn_json)
    # print(type(data))
    df = pd.read_json(json.dumps(data['Snapshots'][0]['Parameter']))

    # print(df.head())
    # print(len(df))
    # hb_cd_dict是HB里面的快照参数dict
    hb_cd_dict = {c['Name']: c['Value'] for _, c in df.iterrows()}
    # print(hb_cd_dict)
    # 获取到HB的Occurrence_Date_Time根据此时间和tbox的采集逻辑来确定查找报文的时间范围
    utc_time_str = data['Occurrence_Date_Time']
    utc_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    utc_dt = datetime.datetime.strptime(utc_time_str, utc_format)
    # occurrence_date_time是UTC时间+8小时
    occurrence_date_time = utc_dt + timedelta(hours=8)
    print('\nHB occurrence_date_time ==',occurrence_date_time)
    # 此条HB报文开始时间和结束时间，可以多找几分钟
    start_time = (occurrence_date_time + timedelta(seconds=start_second)).strftime("%H:%M:%S.%f")
    end_time = (occurrence_date_time + timedelta(seconds=end_second)).strftime("%H:%M:%S.%f")

    # 读取同步性sheet
    df = pd.read_excel('CDtestcase.xlsx', sheetname='同步性', skiprows=[1])

    global spn_dict, pgn_dict
    spn_dict = {}
    pgn_dict = {}
    test_dict = {c['SDK Interface Format v 2.601.002']: c['J1939\nSPN'] for _, c in df.iterrows()} # noqa
    # 根据SDK同步性sheet，test_dict = {快照参数名称：spn}
    # print(test_dict) 
    # pgn_dict = {c['J1939\nPGN']: c['Synchronization Requirement'].replace(u'≤', '').replace('ms', '') for _, c in df.iterrows() if c['Synchronization Requirement'] != u'基准61444'} # noqa
    # 此循环是把HB的快照参数和SDK里面的快照参数进行遍历，找到对应的参数，组合为最新的字典spn_dict = {spn:value}
    for k in test_dict:
        if k in hb_cd_dict.keys():
            spn_dict[test_dict[k]] = hb_cd_dict[k]

    # print(len(spn_dict))
    # print('=======================')
    # 读取SDK里面同步性的pgn和同步性时间，pgn_dict = {pgn:time}
    pgn_dict = {c['J1939\nPGN']: c['Synchronization Requirement'].replace(u'≤', '').replace('ms', '') for _, c in df.iterrows() if c['Synchronization Requirement'] != u'基准61444' and c['J1939\nSPN'] in spn_dict.keys()} # noqa
    # print(len(pgn_dict), pgn_dict.keys())

# 2、开始处理报文
# DataFrame是一种表格型数据结构，它含有一组有序的列，每列可以是不同的值
def read_hex_csv(fn_json):
    PGN61444Dict = {}
    PGN65270Dict = {}
    time_61444 = None
    count_61444 = 0
    hb_continue = False
    # 读取报文hex_message.csv
    df = pd.read_csv('hex_message.csv')
    # print(df['Time'].head())
    # 把Time这列重新插入一列为NewTime，并且把微秒去掉，不影响结果，方便计算
    df['NewTime'] = df['Time']
    df['NewTime'] = df['NewTime'].apply(lambda x: x.rsplit('.', 1)[0])
    # print(df['Time'].head())
    # print(df['NewTime'].head())
    # 根据start_time, end_time确定此HB要查找的报文范围
    global start_time, end_time
    df = df[(df['NewTime'] > str(start_time)) & (df['NewTime'] < str(end_time))]
    # print(df.head())
    # print(df['PGN'].head())
    # row_stat = {HB:0} 存放此条HB的成功率
    row_stat = {'fn_json': fn_json, 'total': 0}
    # 根据pgn=61444进行筛选
    df_61444 = df[(df['PGN'] == 61444)]
    # print(df_61444.head())
    # 开始对61444的报文进行遍历
    for index, row in df_61444.iterrows():
        # 此行61444的报文row['Data']传给hex_pgn_to_spn方法返回对应的dict = {spn:value}，目的是找到此条HB对应的一行61444报文
        find_dict = hex_pgn_to_spn(61444, row['Data'])
        # print(find_dict)
        # print('------',spn_dict.keys() & find_dict.keys())
        # 下面for循环 此条61444报文换算出来的spn对应的dict，跟HB.json里面spn对应的dict进行对比，目的找到跟HB.json里面的对应的一条61444的报文
        equal = True
        for spn_key in find_dict.keys():
            if spn_key not in spn_dict.keys():
                equal = False
                break
            if float(find_dict[spn_key]) != float(spn_dict[spn_key]):
                # print(find_dict[spn_key], spn_dict[spn_key])
                equal = False
                break
        # 如果找到了61444，把值放到time_tuple_61444元组里面，

        if equal:
            PGN61444Dict[row['Time']] = (index, row['Data'])
            time_tuple_61444 = (index, row['Time'], row['Data'])
            print('此条HB 61444的信息', time_tuple_61444)
            hb_continue = True
            # print(time_tuple_61444)
            if len(time_tuple_61444):
                for index, row in df_61444.iterrows():
                    data = time_tuple_61444[2]
                    if row['Data'] == data:
                        count_61444+=1
                print(f'此条HB对应的61444有{count_61444}条')
            break
        else:
            hb_continue = False
    # 找到61444开始找对应的65270报文
    if hb_continue:
        (index, time_61444, data) = time_tuple_61444
        # 65270为开始采集点，在61444的上面找，根据条件把65270筛选出来
        df_65270 = df[(df['PGN'] == 65270) & (df.index < index)]
        # print('*********{}********'.format(df_65270.index))
        nearest_index = min(df_65270.index, key=lambda x: abs(x - index))
        # print(nearest_index)
        # 找到离61444 index最近一条65270找到
        row_65270 = df_65270.loc[nearest_index]
        print('此条HB 开始采集65270的信息{}, {}'.format(row_65270['Time'], row_65270['Data']))
        # DataFrame的行索引是index，列索引是columns，我们可以在创建DataFrame时指定索引的值
        df_stat = pd.DataFrame(columns=['pgn', 'delta', 'success'])
        for pgn in pgn_dict.keys():
            # comapre_pgn此方法是根据61444和65270找到其他pgn
            row_stat = comapre_pgn(df, pgn, nearest_index, time_61444)
            df_stat = df_stat.append(row_stat, ignore_index=True)
        # total总的pgn，success成功的pgn格式
        success = len(df_stat[(df_stat['success'] == True)])
        total = float(len(df_stat))
        print('success = {}, total = {}'.format(success, total))
        percentage = '%.2f' %(success/total)
        print('成功率：{}'.format(percentage))
        row_stat = {'fn_json': fn_json, 'total({})'.format(total): success}
    else:
        print('1、此条HB failed，没有找到对应61444的报文')
        row_stat = {'fn_json': fn_json, 'total': 0}
    return row_stat


def comapre_pgn(df, pgn, index_65270, time_61444):
    # 在index_65270下面的找index
    df_pgn = df[(df['PGN'] == pgn) & (df.index >= index_65270)]
    # if len(df_pgn) == 0:
    #     print(f'此时间内没有找到{pgn}的报文')
        # continue
    # print('{}df_pgn=={}'.format(pgn, len(df_pgn)))
    # 给df_pgn生成新的索引从0开始
    df_pgn.index = range(len(df_pgn))
    not_found = True
    row_stat = None
    # 遍历此pgn的报文
    for index, row in df_pgn.iterrows():
        # FF 00 4D 31 FF FF FF FF
        # if len(row['Data']) > 23:
        #     print('\tlong {}'.format(row['Data']))
        spn_pgn_dict = hex_pgn_to_spn(str(pgn), row['Data'])
        equal = True
        # 如果HB spn不在SDK换算的spn里面，是HB漏报spn，判断此spn的报文是否为无效值（FF），如果是无效值并且没有上报可以视为正确的
        for spn_key in spn_pgn_dict.keys():
            isNA = False
            if spn_key not in spn_dict.keys():
                if spn_key in hex_spn_dict.keys():
                    hex_value = hex_spn_dict[spn_key]
                    string_set = set(list(hex_value.lower()))
                    if (isFliterNA and 'f' in string_set and len(string_set) == 1):
                        # 此参数spn是无效值，不会上报
                        # del spn_pgn_dict[spn_key]
                        isNA = True
                        print(f'spn={spn_key}是无效值')
                    else:
                        equal = False
                        break
                
                else:
                    equal = False
                    break

            # 这4个spn的值是ASCII的直接对比
            if spn_key in (1635, 234, 586, 587) :
                if spn_pgn_dict[spn_key] != spn_dict[spn_key]:
                    equal = False
                    break
            else:
                # HB上报的数据小数点不统一，所以需要把HB的值和报文算出来的值转换为float，2个值的差的绝对值小于0.01是
                if (not isNA) and (abs(float(spn_pgn_dict[spn_key]) - float(spn_dict[spn_key])) >= 0.01):
                    equal = False
                    break

        # 如果找到对应的报文，计算此png跟61444的时间差
        if equal:
            not_found = False
            time_a = row['Time'].rsplit('.', 1)[0]
            time_b = time_61444.rsplit('.', 1)[0]
            time_format = '%H:%M:%S.%f'
            time_a = datetime.datetime.strptime(time_a, time_format)
            time_b = datetime.datetime.strptime(time_b, time_format)

            # print(time_a, time_b)
            delta = abs(time_b - time_a).seconds * 1000 + abs(time_b - time_a).microseconds / 1000

            global pgn_dict

            fail_spn_dict = {}
            for item in spn_pgn_dict:
                if item in spn_dict.keys():
                    fail_spn_dict[item] = spn_dict[item]
                else:
                    fail_spn_dict = {}

            if pgn in pgn_dict and delta <= int(pgn_dict[pgn]):
                row_stat = {'pgn': pgn, 'delta': delta, 'success': True}
                print('\tcompare pgn {} success time {} <= {}  65270下面广播的第{}条报文 ecm={} <==> hb={}'.format(pgn, delta, pgn_dict[pgn], index+1, spn_pgn_dict, fail_spn_dict))
                break
            else:
                row_stat = {'pgn': pgn, 'delta': delta, 'success': False}
                print('\tcompare pgn {} fail time {} > {}     65270下面广播的第{}条报文 ecm={} <==> hb={}'.format(pgn, delta, pgn_dict[pgn], index+1, spn_pgn_dict, fail_spn_dict))
                # 根据索引获取取到的是第几个报文
                # print('======={}========'.format(index+1))
                break
    if not_found:
        row_stat = {'pgn': pgn, 'delta': None, 'success': False}
        print('\tcompare pgn {} not found'.format(pgn))
    return row_stat


# 根据pgn和报文，计算出对应spn的值，select_spn_dict = {spn:value}
# 无效值FF放到hex_spn_dict
def hex_pgn_to_spn(pgn, hexStr):
    sPGN = str(pgn)
    # print(len(hexStr))
    # 把16进制的报文，转换为字节数组
    hexArray = np.array(hexStr.split())
    # print(len(hexArray))
    # txt文件和当前脚本在同一目录下，所以不用写具体路径
    filename = 'rules.txt' 
    rowField = []
    select_spn_dict = {}
    # 读取spn计算规则的文本文件，一行一行读取
    with open(filename, 'r') as file:
        for x in file:
            pgnRow = []
            pgnRowArray = []
            # 先判断传过来的pgn是否是此行的pgn
            if sPGN in x:
                # 去掉结尾的换行符
                x = x.strip('\n')
                # 按照制表符'\t'切割字符串，得到的结果构成一个数组
                pgnRow.append(x.split('\t'))
                pgnRowArray = np.array(pgnRow)[0]
                # 找出此数组里面的对应的spn，字节位置，长度，resolution，offset
                spn = pgnRowArray[1]
                Bpostion = pgnRowArray[2]
                unit = pgnRowArray[3]
                length = (pgnRowArray[3].split())[0]
                resolution = pgnRowArray[4].split()[0]
                offset = pgnRowArray[5].split()[0]
                # 1、需要转换为ASCII的参数，单独处理
                if 'ASCII' in resolution:
                    if spn == '234':
                        hexStr1 = (''.join(hexArray[1:]))
                    elif spn == '1635':
                        hexStr1 = (''.join(hexArray[4:19]))
                    elif sPGN == '65259':
                        hexStr1 = (''.join(hexArray[:]))
                    # print('...', type(hexStr1))
                    # print(spn)
                    # print('hex', hexStr1)
                    # if spn == '1635':
                    #     spnResult = bytes.fromhex(hexStr1.strip('0')+'0').decode()
                    # else:
                    #     spnResult = bytes.fromhex(hexStr1.strip('0')).decode()
                    spnResult = bytes.fromhex(hexStr1.strip('0')).decode()


                    if spn == '586':
                        spnResult = spnResult.split('*')[0]
                    elif spn == '587':
                        spnResult = spnResult.split('*')[1]
                    select_spn_dict[int(spn)] = str(spnResult)
                    # print('select_spn_dict:', select_spn_dict)
                else:
                    # 2、以byte为单位的
                    if 'byte' in unit:
                        postionArr = []
                        hexArray2 = []
                        if '-' in Bpostion:
                            postionArr = np.array(Bpostion.split('-'))
                            hexArray2 = hexArray[(int(postionArr[0])-1):int(postionArr[1])]
                            # print(hexArray2)
                            hexArray2 = hexArray2[::-1]
                            # print(hexArray2)
                            # string1 = '0x' + (''.join(hexArray2))
                            hexStr1 = (''.join(hexArray2))
                            decNum = int(hexStr1, 16)
                            if '/' in resolution:
                                spnResult = decNum * float(Fraction(resolution)) + float(offset)
                            else:
                                spnResult = decNum * float(resolution) + float(offset)

                            select_spn_dict[int(spn)] = str(spnResult)
                            # print('select_spn_dict:', select_spn_dict)
                        else:
                            hexStr1 = hexArray[int(Bpostion)-1]
                            decNum = int(hexStr1, 16)
                            spnResult = decNum * float(resolution) + float(offset)
                            select_spn_dict[int(spn)] = str(spnResult)
                            # print('select_spn_dict:', select_spn_dict)
                    else:
                        # 3、以为bit为单位的
                        # print('Bpostion===',Bpostion)
                        Bytepos, binpos = Bpostion.split('.')
                        hexStr1 = hexArray[int(Bytepos)-1]
                        decNum = int(hexStr1, 16)
                        binNum = '{:08b}'.format(decNum)
                        # print('binNum:',binNum, type(binNum))
                        binbegin = -int(binpos)+1
                        binend = -int(length) + (-int(binpos)) + 1
                        # print(binbegin)
                        # print(binend)

                        if int(binpos) == 1:
                            binResult = binNum[binend:]
                            # print(binNum[binend:])
                        else:
                            binResult = binNum[binend:binbegin]
                            # print(binNum[binend:binbegin])

                        if 'states' in pgnRowArray[4]:
                            binResult = int(binResult, 2)
                            select_spn_dict[int(spn)] = str(binResult)
                            # print('select_spn_dict:', select_spn_dict)
                        else:
                            binResult = int(binResult, 2) * float(resolution) + float(offset)
                            select_spn_dict[int(spn)] = str(binResult)
                            # print('select_spn_dict:', select_spn_dict)
    # print('*********', select_spn_dict)
    return select_spn_dict
                    


def test_all():
    global json_list, isFliterNA, start_second, end_second
    # 是否过滤无效值
    isFliterNA = False
    # 根据HB的OccurrenceDateTime，找出此条HB的报文采集时间段，start_second=-5，代表在OccurrenceDateTime的时间再往前找5秒，end_second = 25，代表在OccurrenceDateTime时间往后找25秒
    start_second = -5
    end_second = 25
    df = pd.read_csv('ProcessedHBMessagesDFAC20210408.csv')

    start_hb = '2021-04-08T09:31:57.000Z'
    end_hb = '2021-04-08T10:02:57.000Z'

    # start_hb = '2021-04-08T08:57:57.000Z'
    # end_hb = '2021-04-08T09:26:57.000Z'


    df = df[(df['OccurrenceDateTime'] >= str(start_hb)) & (df['OccurrenceDateTime'] <= str(end_hb))]
    
    for index, row in df.iterrows():
        json_list.append(row['ProcessedMessage'])

    # 新建一个统计 DataFrame 有 2 列：文件名、成功率
    df_per = pd.DataFrame(columns=['fn_json', 'percentage'])
    for x in range(len(json_list)):
        fn_json = json_list[x]
        json_name = 'HB' + str(x+1)
        # 读取 json 文件
        read_hb_json(fn_json)
        # 计算成功率
        row_per = read_hex_csv(json_name)
        # 结果写入 DataFrame
        df_per = df_per.append(row_per, ignore_index=True)
        
    # 保存为 csv 文件，后续绘图使用其中的数据
    df_per.to_csv('df_per.csv', index=False)


if __name__ == '__main__':

    # 程序的入口，开始处理HB和报文
    test_all()
