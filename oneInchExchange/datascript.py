import os
import os.path
from os import path
import requests, json, csv
from datetime import datetime

ssh_path = './data/'
oneInchSubgraphURL = 'https://api.thegraph.com/subgraphs/name/1inch-exchange/oneinch-liquidity-protocol-v2'
ethereumBlocksSubgraphURL = 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks'

blockNumbersTimestamp = []


def _main():
    print('CHECK')
    # getDates(1609977600)
    return populateUserData24HrIntervals()


def populateUserData24HrIntervals():
    return deployScript()


def deployScript():
    pairIds = getPairsList()
    print('Pair list details fetched and data written in data/AllPairsList.csv file ')

    getDailyDataForEachPair()


def getDates(date):
        _path = './data/datesList.csv'
        with open(_path, 'w', newline='') as file:
            for i in range(0,30):
                writer = csv.writer(file)
                writer.writerow([date + (3*i*86400)])


def getPairsList():
    query = """  query($skipNum: Int) {
                    pairs(first:500, skip:$skipNum, orderBy: createdAtTimestamp) {
                        id
                        createdAtTimestamp
                        createdAtBlockNumber
                        token0 {
                            name
                            symbol
                        }
                        token1 {
                            name
                            symbol
                        }                        
                    }
            }"""
    pairIds = []
    paidIdDetails = []
    skipNum = 0
    totalPairs = 0
    while(1):
        variables = {'$skipNum': skipNum}
        r = requests.post(oneInchSubgraphURL, json={'query': query, 'variables': variables})
        pairs_data = json.loads(r.text)['data']['pairs']
        for pair in pairs_data:
            pairIds.append(pair['id'])
            paidIdDetails.append(pair)
            # print(pair)

        totalPairs = totalPairs + len(pairs_data)
        if len(pairs_data) < 500:
            break
        skipNum = 500

    print('total pairs = ' + str(totalPairs))
    writePairIdList(paidIdDetails)
    createFilesForPairsDailyData(paidIdDetails)
    return pairIds


def createFilesForPairsDailyData(pairIdDetails):
    _path = './data/AllPairsDailyData/'
    for pairDetails in pairIdDetails:
        # print(pairDetails)
        fileName = pairDetails['token0']['symbol']  + '-' + pairDetails['token1']['symbol']  + ':' + pairDetails['id']
        if not path.exists(_path + fileName):
            with open(_path + fileName , 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['id', 'pairAddress', 'date','timestamp', 'token 0 (Name)', 'token 0 (Symbol)',
                                'token 1 (Name)', 'token 1 (Symbol)','totalSupply','reserve 0',
                                'reserve 1','reserveUSD','Token 0 : dailyVolume','Token 1 : dailyVolume',
                                'dailyVolume (USD)','daily Txns'])
                print(fileName + ' created successfully' )


def writePairIdList(pairListDetails):
    path = './data/'
    with open(path + '/AllPairsList.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id', 'createdAtTimestamp', 'createdAtBlockNumber', 'token 0 (Name)', 'token 0 (Symbol)', 'token 1 (Name)', 'token 1 (Symbol)'])
        # print('new file for Instrument Configs made with path: ' + path + '/instrumentConfigs.csv')

    with open(path + '/AllPairsList.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        for pair in pairListDetails:
            writer.writerow([pair['id'],pair['createdAtTimestamp'],pair['createdAtBlockNumber'],pair['token0']['name'],pair['token0']['symbol'],pair['token1']['name'],pair['token1']['symbol']])




def getDailyDataForEachPair():
    query = """  query($skipNum:Int) {
                    pairDayDatas(first:1000,  orderBy: date, where: {date_gte: 1617494400} ) {
                        pairAddress
                        id
                        date
                        token0 {
                            name
                            symbol
                        }
                        token1 {
                            name
                            symbol
                        }  
                        totalSupply 
                        reserve0
                        reserve1    
                        reserveUSD                 
                        dailyVolumeToken0
                        dailyVolumeToken1
                        dailyVolumeUSD
                        dailyTxns
                    }
            }"""
    returntData_ = []
    r = requests.post(oneInchSubgraphURL, json={'query': query} )
    # print(r.text)
    if json.loads(r.text):
        if json.loads(r.text)['data']:
            returntData_ = json.loads(r.text)['data']['pairDayDatas']
    for pair in returntData_:
        if pair['date'] < 1617494400 + (3*86400):
            print(str(pair['date']) + ' ' + str(pair['id']) + ' ' + str(pair['pairAddress']))
            writeDailyDataForPairInItsFile(pair)

    return


def writeDailyDataForPairInItsFile(pairData):
    path = './data/AllPairsDailyData/'
    fileName = pairData['token0']['symbol'] + '-' + pairData['token1']['symbol'] + ':' + pairData['pairAddress']
    with open(path + fileName, 'a', newline='') as file:
        print(fileName)
        writer = csv.writer(file)
        writer.writerow([pairData['id'], pairData['pairAddress'],datetime.utcfromtimestamp(int(pairData['date'])).strftime('%Y-%m-%d %H:%M:%S'), pairData['date'], pairData['token1']['name'], pairData['token1']['symbol'],
                         pairData['token1']['name'], pairData['token1']['symbol'], pairData['totalSupply'], pairData['reserve0'],
                         pairData['reserve1'],pairData['reserveUSD'], pairData['dailyVolumeToken0'], pairData['dailyVolumeToken1'],
                         pairData['dailyVolumeUSD'], pairData['dailyTxns']])





# def getTokenDayData():
#     query = """  query($skipNum: Int) {
#                     mooniswapDayDatas(first:100, skip:$skipNum) {
#                         id
#                     }
#             }"""
#
#     skipNum = 0
#     variables = {'$skipNum': skipNum }
#     r = requests.post(oneInchSubgraphURL, json={'query': query, 'variables': variables})
#     print(r.text)
#     user_data = json.loads(r.text)['data']['PairDayDatas']
#     print(' \n \n')
#     print(user_data)
#     return user_data






if __name__ == '__main__':
    _main()