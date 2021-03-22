import os
import requests, json, csv
from brownie import accounts, Contract, network, LendingPool
from datetime import datetime

# lendingProtocol = Contract.from_abi('ILendingPool', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9', LendingPool.abi)
aaveV2SubgraphURL = 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2'
ethereumBlocksSubgraphURL = 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks'

blockNumbersTimestamp = []


def main():
    return populateUserData24HrIntervals()


def populateUserData24HrIntervals():
    return deployScript()


def deployScript():
    loadBlockNumbersList(1609698600, 1)
    print('Block numbers fetched for ' + str(len(blockNumbersTimestamp)) + ' days since 1609981200')

    for day in blockNumbersTimestamp:
        print('\n \n')
        print('time : ' + datetime.utcfromtimestamp(int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S') + ' Block Number : ' + day['number'])
        createFolderForANewDay(day['timestamp'])
        userAddressesList = getUserAddresses(day['number'])
        print('Total User Addresses Retrieved = ' + str(len(userAddressesList)))
        instrumentSymbols, instrumentConfigs = getInstrumentsConfigForABlock(
            day['number'])  # fetches Instrument Config values for the current Block Number
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
            print( str(i) + 'Data written successfully for ' + user + ' for timestamp = ' + datetime.utcfromtimestamp(int(day['timestamp'])).strftime('%Y-%m-%d %H:%M:%S'))
            i = i + 1


        print('\n \n')

    # with open('userData24HrIntervals.csv', 'w', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(["Timestamp", "User Address", "Health Factor", "Total Collateral (ETH)", "Available Borrows (ETH)","Total Debt (ETH)","Current Liquidation Threshold", "LTV"])


def getUserBalancesRowWithComputedValues(instrumentConfigs, user_reserve_balances):
    totalDepositBalanceETH = 0
    totalCollateralETH = 0
    totalLiquidationThresholdETH = 0
    totalDebtETH = 0
    for reserve in user_reserve_balances['reserves']:
        symbol = reserve['reserve']['symbol']
        # print(symbol + ' = ' + str(reserve) + ' || Price (ETH) : ' + str(int(instrumentConfigs[symbol]['price']['priceInEth']) / pow(10,18)) )
        #  decimals corrected (both price and instrument balance)
        curInstrBalanceETH = (int(reserve['currentATokenBalance']) / pow(10, int(
            instrumentConfigs[symbol]['decimals']))) * (
                                         int(instrumentConfigs[symbol]['price']['priceInEth']) / pow(10, 18))
        curInstrDebtETH = (int(reserve['currentTotalDebt']) / pow(10, int(instrumentConfigs[symbol]['decimals']))) * (
                    int(instrumentConfigs[symbol]['price']['priceInEth']) / pow(10, 18))
        totalDepositBalanceETH = totalDepositBalanceETH + curInstrBalanceETH
        totalDebtETH = totalDebtETH + curInstrDebtETH
        if reserve['usageAsCollateralEnabledOnUser']:
            totalCollateralETH = totalCollateralETH + (
                        curInstrBalanceETH * (int(instrumentConfigs[symbol]['baseLTVasCollateral']) / 10000))
            totalLiquidationThresholdETH = totalLiquidationThresholdETH + curInstrBalanceETH * (
                        int(instrumentConfigs[symbol]['reserveLiquidationThreshold']) / 10000)

    averageLiquidationThreshold = 0
    if int(totalCollateralETH) > int(0):
        averageLiquidationThreshold = int(totalLiquidationThresholdETH) / int(totalCollateralETH)
    healthFactor = 1000000
    if int(totalDebtETH) > 0:
        healthFactor = (int(totalCollateralETH) * averageLiquidationThreshold) / totalDebtETH
    return totalDepositBalanceETH, totalCollateralETH, totalDebtETH, totalLiquidationThresholdETH, averageLiquidationThreshold, healthFactor


'''
Description : Get balances for all the reserves for a user at a particular block number
'''


def getUserData(userAddress, _blockNumber):
    query = """ query($ID: String , $blockNumber: Int) {
      user(id: $ID, block:{number: $blockNumber}) {
        borrowedReservesCount
        reserves {
          reserve {
            symbol
          }
          usageAsCollateralEnabledOnUser
          currentATokenBalance
          currentVariableDebt
          currentStableDebt
          currentTotalDebt
        }
      }
    }"""
    variables = {'ID': userAddress, 'blockNumber': int(_blockNumber)}
    r = requests.post(aaveV2SubgraphURL, json={'query': query, 'variables': variables})
    user_data = json.loads(r.text)['data']['user']
    return user_data


def getInstrumentsConfigForABlock(_blockNumber):
    query = """ query($blockNumber: Int) {
                    reserves(block:{number: $blockNumber} ) {
                        symbol
                        name
                        decimals
                        price {
                          priceInEth
                        }
                        baseLTVasCollateral
                        reserveLiquidationThreshold
                        reserveLiquidationBonus
                    }   
                }"""
    variables = {'blockNumber': int(_blockNumber)}
    r = requests.post(aaveV2SubgraphURL, json={'query': query, 'variables': variables})
    instruments_data_ = json.loads(r.text)['data']['reserves']
    instruments_data_returned = {}
    instrumentSymbols = []
    for reserve in instruments_data_:
        instruments_data_returned[reserve['symbol']] = reserve
        instrumentSymbols.append(reserve['symbol'])

    return instrumentSymbols, instruments_data_returned


'''
    description: Fetch addresses of the users participating in Aave Protocol
'''


def getUserAddresses(_blockNumber):
    query = """ query($skipNum: Int, $blockNumber: Int) {
            users(first: 1000, skip: $skipNum , block:{number: $blockNumber} ) {
            id
            }    
        }"""
    skip = 0
    userAddressesList = []

    while (1):
        variables = {'skipNum': skip, 'blockNumber': int(_blockNumber)}
        r = requests.post(aaveV2SubgraphURL, json={'query': query, 'variables': variables})
        returnedVal = json.loads(r.text)
        if 'errors' in returnedVal:
            break
        users_data = returnedVal['data']['users']
        print('Skip = ' + str(skip) + ' userAddresses = ' + str(len(users_data)))
        if len(users_data) == 0:
            break
        for user in users_data:
            userAddressesList.append(user['id'])
        skip = skip + 1000

    return userAddressesList


'''
    description: Populates the global 'blockNumbersTimestamp' which stores blockNumber and the timestamp for that block Number
'''


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


# CREATES A FOLDER WITHIN THE 'AAVEDATA' dir. for a new day
def createFolderForANewDay(timestamp):
    date = '/Users/apple/Desktop/DelphiDigital/AaveDataScripts/userDataExtraction@4HrGMT/aaveData/' + datetime.utcfromtimestamp(
        int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    if not os.path.exists(date):
        os.mkdir(date)


# WRITES INSTRUMENT CONFIGURATION DATA FOR A DAY
def writeInstrumentConfigsForADay(timestamp, instrumentConfigs, instrumentSymbols):
    path = '/Users/apple/Desktop/DelphiDigital/AaveDataScripts/userDataExtraction@4HrGMT/aaveData/' + datetime.utcfromtimestamp(
        int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    with open(path + '/instrumentConfigs.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['Reserve Symbol', 'Name', 'decimals', 'price (ETH)', 'LTV', 'Liquidation Threshold', 'Liquidation Bonus'])
    for instrument in instrumentSymbols:
        with open(path + '/instrumentConfigs.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                [instrument, instrumentConfigs[instrument]['name'], instrumentConfigs[instrument]['decimals'],
                 instrumentConfigs[instrument]['price']['priceInEth'],
                 instrumentConfigs[instrument]['baseLTVasCollateral'],
                 instrumentConfigs[instrument]['reserveLiquidationThreshold'],
                 instrumentConfigs[instrument]['reserveLiquidationBonus']])


# CREATES THE FILE WHICH WILL STORE USER BALANCES FOR A DAY
def createUserDataFileForANewDay(timestamp, instrumentSymbols):
    path = '/Users/apple/Desktop/DelphiDigital/AaveDataScripts/userDataExtraction@4HrGMT/aaveData/' + datetime.utcfromtimestamp(
        int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    firstRow = ['User Address', 'healthFactor', 'totalDepositBalanceETH', 'totalDebtETH', 'totalCollateralETH',
                'totalLiquidationThresholdETH', 'averageLiquidationThreshold']

    for symbol in instrumentSymbols:
        firstRow.append(symbol + ' Total Deposit Balance')
        firstRow.append(symbol + ' Total Borrow Balance')
        firstRow.append(symbol + ' Stable Borrow Balance')
        firstRow.append(symbol + ' Variable Borrow Balance')
        firstRow.append(symbol + ' is used as Collateral')

    with open(path + '/userBalances.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(firstRow)


# ADDS USER BALANCES ROW TO THE CURRENT FILE
def addUserRowToCurrentFile(timestamp, user, instrumentSymbols, user_reserve_balances, totalDepositBalanceETH,
                            totalCollateralETH, totalDebtETH, totalLiquidationThresholdETH, averageLiquidationThreshold,
                            healthFactor):
    path = '/Users/apple/Desktop/DelphiDigital/AaveDataScripts/userDataExtraction@4HrGMT/aaveData/' + datetime.utcfromtimestamp(
        int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    userRow = [user, healthFactor, totalDepositBalanceETH, totalDebtETH, totalCollateralETH,
               totalLiquidationThresholdETH, averageLiquidationThreshold]
    userDataMap = {}
    instrumentBalancesCheck = {}

    for symbol in instrumentSymbols:
        instrumentBalancesCheck[symbol] = False
        for reserve in user_reserve_balances['reserves']:
            instrumentBalancesCheck[reserve['reserve']['symbol']] = True
            if symbol == reserve['reserve']['symbol']:
                userDataMap[symbol] = reserve

    for symbol in instrumentSymbols:
        if instrumentBalancesCheck[symbol]:
            userRow.append(userDataMap[symbol]['currentATokenBalance'])
            userRow.append(userDataMap[symbol]['currentTotalDebt'])
            userRow.append(userDataMap[symbol]['currentStableDebt'])
            userRow.append(userDataMap[symbol]['currentVariableDebt'])
            userRow.append(userDataMap[symbol]['usageAsCollateralEnabledOnUser'])
        else:
            userRow.append(0)
            userRow.append(0)
            userRow.append(0)
            userRow.append(0)
            userRow.append('False')

    with open(path + '/userBalances.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(userRow)
