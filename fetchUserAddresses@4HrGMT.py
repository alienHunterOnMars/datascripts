import os
import requests, json, csv
# from brownie import Contract, network, LendingPool
from datetime import datetime

# lendingProtocol = Contract.from_abi('ILendingPool', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9', LendingPool.abi)
# ssh_path = f"{os.getenv('HOME')}/scripts/AaveScripts/userDataExtraction@4HrGMT/"
aaveV2SubgraphURL = 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2'
ethereumBlocksSubgraphURL = 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks'

blockNumbersTimestamp = []
userAddressesList = []
userAddressesDict = {}


def _main():
    return populateUserAddresses24HrIntervals()


def populateUserAddresses24HrIntervals():
    return deployScript()


def deployScript():
    loadBlockNumbersList(1608076800, 111)
    # print(datetime.utcfromtimestamp(int(1608076800 + 70*86400)).strftime('%Y-%m-%d %H:%M:%S'))
    print('Block numbers fetched for ' + str(len(blockNumbersTimestamp)) + ' days since 1609981200')
    prevTimeStamp = 0

    for day in blockNumbersTimestamp:
        print('\n \n')
        print('time : ' + datetime.utcfromtimestamp(int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S') + ' Block Number : ' + day['number'])
        newUserAddresses = getUserAddresses(day['number'],prevTimeStamp)
        print('Total NEW User Addresses Retrieved = ' + str(len(newUserAddresses)))
        print('Total User Addresses Retrieved for the day ' + datetime.utcfromtimestamp(int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')  + ' is = ' + str(len(userAddressesList)))
        writeAddressesForADay(day['timestamp'])
        # print('User Addresses Data Written Successfully to the file')
        prevTimeStamp = int(day['timestamp'])





# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
def loadBlockNumbersList(_timestamp, numberOfDays):
    query = """ query($timestamp: Int)   {
                    blocks(first: 1, orderBy: timestamp, orderDirection: asc, where: {timestamp_gt: $timestamp}) {
                        number
                        timestamp
                    }
                }"""
    for i in range(0, numberOfDays):
        timestamp = _timestamp + (i * 86400)  # 24 hours = 86400
        variables = {'timestamp': timestamp}
        r = requests.post(ethereumBlocksSubgraphURL, json={'query': query, 'variables': variables})
        print(datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S'))
        if not json.loads(r.text)['data']:
            break
        print(json.loads(r.text))
        blockData = json.loads(r.text)['data']['blocks']
        if len(blockData) == 0:
            break
        blockNumbersTimestamp.append(blockData[0])
    return


# GETS NEW USER ADDRESSES WHICH JOINED AAVE
def getUserAddresses(_blockNumber, _prevTimestamp):
    query = """ query($skipNum:Int, $prevTimestamp: Int, $blockNumber: Int) {
                userReserves(first: 1000,  skip:$skipNum, orderBy: lastUpdateTimestamp, where: {lastUpdateTimestamp_gt: $prevTimestamp}, block:{number: $blockNumber} ) {
                    user {
                        id
                    }
                }    
            }"""
    skip = 0
    newUserAddressesList = []

    while (1):
        variables = {'skipNum': skip, 'prevTimestamp': int(_prevTimestamp) ,'blockNumber': int(_blockNumber),  }
        r = requests.post(aaveV2SubgraphURL, json={'query': query, 'variables': variables})
        returnedVal = json.loads(r.text)
        # print(returnedVal)
        # if 'errors' in returnedVal:
        #     break
        users_data = returnedVal['data']['userReserves']
        print('Skip = ' + str(skip) + ' userReserves = ' + str(len(users_data)))
        if len(users_data) == 0:
            break
        for user in users_data:
            address = user['user']['id']
            if address not in userAddressesDict:
                newUserAddressesList.append(address)
                userAddressesList.append(address)
                userAddressesDict[address] = True

        skip = skip + 1000

    # print(newUserAddressesList)
    return newUserAddressesList


def writeAddressesForADay(_timestamp):
    path = './addresses/'
    with open(path + str(_timestamp) + '.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for userAddress in userAddressesList:
            writer.writerow([userAddress])



if __name__ == '__main__':
    print('INITIATING SCRIPT')
    # print(ssh_path)
    _main()