import os
import requests, json, csv
# from brownie import Contract, network, LendingPool
from datetime import datetime

# lendingProtocol = Contract.from_abi('ILendingPool', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9', LendingPool.abi)
ssh_path = f"{os.getenv('HOME')}/scripts/AaveScripts/userDataExtraction@4HrGMT/"
aaveV2SubgraphURL = 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2'
ethereumBlocksSubgraphURL = 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks'

blockNumbersTimestamp = []

def _main():
    return populateUserAddresses24HrIntervals()


def populateUserAddresses24HrIntervals():
    return deployScript()


def deployScript():
    loadBlockNumbersList(1611100800, 3)
    print('Block numbers fetched for ' + str(len(blockNumbersTimestamp)) + ' days since 1609981200')

    for day in blockNumbersTimestamp:
        print('\n \n')
        print('time : ' + datetime.utcfromtimestamp(int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S') + ' Block Number : ' + day['number'])
        createFolderForANewDay(day['timestamp'])
        userAddressesList = getUserAddresses(day['number'])
        print('Total User Addresses Retrieved = ' + str(len(userAddressesList)))
        instrumentSymbols, instrumentConfigs = getInstrumentsConfigForABlock(day['number'])  # fetches Instrument Config values for the current Block Number
        writeInstrumentConfigsForADay(day['timestamp'], instrumentConfigs, instrumentSymbols)
        print('Instrument Configuration Data Written Successfully to the file')
        createUserDataFileForANewDay(day['timestamp'], instrumentSymbols)
        print('File to store user Balances created Successfully')
        i = 0
        for user in userAddressesList:
            # if i <= :
            #     i = i+1
            #     continue
            user_reserve_balances = getUserData(user, day['number'])
            totalDepositBalanceETH, totalCollateralETH, totalDebtETH, totalLiquidationThresholdETH, averageLiquidationThreshold, healthFactor = getUserBalancesRowWithComputedValues(
                instrumentConfigs, user_reserve_balances)
            addUserRowToCurrentFile(day['timestamp'], user, instrumentSymbols, user_reserve_balances,
                                    totalDepositBalanceETH, totalCollateralETH, totalDebtETH,
                                    totalLiquidationThresholdETH, averageLiquidationThreshold, healthFactor)
            print(
                str(i) + ' => Data written successfully for ' + user + ' for timestamp = ' + datetime.utcfromtimestamp(
                    int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S'))
            i = i + 1

        print('\n \n')




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
