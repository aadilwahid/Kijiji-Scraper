import os
from os import path
from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
# import pandas as pd
import csv

logFileName = "Logs_" + datetime.now().strftime("%m-%d-%Y__%H-%M-%S") + ".txt"
baseURLs = []
baseUrl = 'https://www.kijiji.ca'
results = []

csvColumns = ['Province', 'Date Scraped', 'Date Posted', 'Title', 'Address', 'Price', 'Type', 'Bedrooms', 'Bathrooms', 'Description', 'Utilities Included', 'Wi-Fi And More', 'Parking Included', 'Agreement Type', 'Move-In Date', 'Pet Friendly', 'Size',
              'Furnished', 'Appliances', 'Air Conditioning', 'Personal Outdoor Space', 'Smoking Permitted', 'Amenties', 'Elevator Accessibility Features', 'Barrier-free Entrances and Ramps', 'Visual Aids', 'Accessible Washrooms in Suite', 'Visits', 'Kijiji Url']


class Record:
    def __init__(self):
        self.province = ''
        self.dateScraped = datetime.now().strftime("%m-%d-%Y__%H-%M-%S")
        self.datePosted = ''

        self.title = ''
        self.address = ''
        self.price = ''
        self.type = ''
        self.bedrooms = ''
        self.bathrooms = ''
        self.description = ''
        self.utilitiesIncluded = ''
        self.wifiAndMore = ''
        self.parkingIncluded = ''
        self.agreementType = ''
        self.moveInDate = ''
        self.petFriendly = ''
        self.size = ''
        self.furnished = ''
        self.appliances = ''
        self.airConditioning = ''
        self.personalOutdoorSpace = ''
        self.smokingPermitted = ''
        self.amenities = ''
        self.elevatorAccessibilityFeatures = ''
        self.barrierFreeEntrancesAndRamps = ''
        self.visualAids = ''
        self.accessibleWashroomsInSuite = ''
        self.visits = ''

        self.kijijiURL = ''

    def __str__(self):
        return self.province + ',' + self.dateScraped + ',' + datetime.now().strftime("%m-%d-%Y__%H-%M-%S") + ',' + self.datePosted + ',' + self.title + ',' + self.address + ',' + self.price + ',' + self.type + ',' + self.bedrooms + ',' + self.bathrooms + ',' + self.description + ','+self.utilitiesIncluded + ',' + self.wifiAndMore + ',' + self.parkingIncluded + ','+self.agreementType + ',' + self.moveInDate + ','+self.petFriendly + ','+self.size + ','+self.furnished + ','+self.appliances + ','+self.airConditioning + ','+self.personalOutdoorSpace + ','+self.smokingPermitted + ','+self.amenities + ','+self.elevatorAccessibilityFeatures + ','+self.barrierFreeEntrancesAndRamps + ',' + self.visualAids + ','+self.accessibleWashroomsInSuite + ',' + self.visits + ','+self.kijijiURL


def getProvinceName(url):
    province = ''

    try:
        splitted = url.split('/')

        if len(splitted) > 4:
            province = splitted[4].replace('-', ' ').title()
    except Exception as ex:
        log(f'getProvinceName()=> {str(ex)}')

    return province


def makeHttpRequest(url, ifSaveResponse):
    retry = 0
    responseTxt = ''

    while retry < 3 and len(responseTxt).__eq__(0):
        try:
            log('...')
            log('Navigating to following URL:')
            log(url)

            response = requests.get(url)
            responseTxt = response.text

            if ifSaveResponse:
                writeLastResponse(responseTxt)
        except Exception as ex:
            retry += 1
            log("makeHttpRequest()=> " + str(ex))
            log('...RETRYING...')
            time.sleep(2)

    return responseTxt


def writeLastResponse(response):
    with open("Last Response/lastResponse.txt", "w") as file:
        file.write(response)


def log(message):
    data = datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -> " + message + "\n"
    print(data)

    with open(logFileName, "a") as logFile:
        logFile.write(data)


def startScraping():
    if path.exists("urls.txt"):
        with open("urls.txt", "r") as file:
            baseURLs = file.readlines()

        # Filter URLs, so that empty lines are ignored.
        baseURLs = [url for url in baseURLs if url.strip()]

        if not path.exists("Results"):
            os.mkdir("Results")

        if not path.exists("Last Response"):
            os.mkdir("Last Response")

        log(f"{len(baseURLs)} URL(s) loaded")
        log("Starting to SCRAPE...")

        # Iterate over each URL.
        for index, url in enumerate(baseURLs, 1):
            province = getProvinceName(url)
            log(f"{index}- {province} | {url}")

            responseTxt = makeHttpRequest(url, True)
            soup = BeautifulSoup(responseTxt, "html.parser")

            try:
                node = soup.select_one(".showing")
                if node:
                    results.clear()
                    log("...")
                    noOfResults = node.text.replace(
                        "showing", "").replace("results", "").strip()
                    noOfResults = noOfResults.split('f')[1].strip()
                    log(f"{noOfResults} results found.")
                    log("...")

                    # get the advertisement links on the page.
                    adUrls = soup.find_all("div", class_="search-item")
                    while adUrls and len(adUrls) > 0:
                        scrapeGivenRecords(adUrls, province)

                        adUrls.clear()
                        retry = 0

                        nextPage = soup.find('a', {'title': 'Next'})
                        if nextPage:
                            time.sleep(1)

                            nextPageUrl = baseUrl + nextPage.attrs["href"]

                            while len(adUrls) == 0 and retry < 3:
                                try:
                                    responseTxt = makeHttpRequest(
                                        nextPageUrl, True)
                                    soup = BeautifulSoup(
                                        responseTxt, "html.parser")
                                    adUrls = soup.find_all(
                                        "div", class_="search-item")

                                    time.sleep(2)

                                except Exception as ex:
                                    log("While navigating to next page" + str(ex))
                                finally:
                                    retry += 1

                    exportToCSV(province)
                else:
                    log("No ads found.")
            except Exception as ex:
                log("startScraping()=> " + str(ex))
    else:
        log("No url.txt found.")


def scrapeGivenRecords(adUrls, province):
    for link in adUrls:
        adlink = baseUrl + link.attrs["data-vip-url"]
        scrapeAd(adlink, province)


def scrapeAd(url, province):
    found = [item for item in results if item.kijijiURL.__eq__(url)]
    if found:
        log("*** DUPLICATE DETECTED ***")
        return

    time.sleep(1)

    responseTxt = makeHttpRequest(url, False)
    soup = BeautifulSoup(responseTxt, "html.parser")

    try:
        node = soup.select_one('h1[class*="title-"]')
        if node:
            record = Record()
            record.province = province
            record.kijijiURL = url
            record.title = node.text.replace("&amp;", "&")

            #########
            # price #
            #########
            node = soup.select_one('div[class*="priceWrapper-"]>span')
            if node:
                record.price = node.text.strip()

            ###########
            # address #
            ###########
            node = soup.select_one('span[class*="address-"]')
            if node:
                record.address = node.text.strip()

            ##############
            # datePosted #
            ##############
            node = soup.select_one('div[class*="datePosted-"]>time')
            if not node:
                node = soup.select_one('div[class*="datePosted-"]>span')

            if node:
                record.datePosted = node.text.strip()

            ############
            # unit row #
            ############
            node = soup.select('div[class*="unitRow-"]>div>li>span')
            if node:
                record.type = node[0].text.strip()

                if len(node) >= 2:
                    record.bedrooms = node[1].text.strip()

                if len(node) >= 3:
                    record.bathrooms = node[2].text.strip()

            ###############
            # description #
            ###############
            node = soup.select_one('div[class*="descriptionContainer-"]>div')
            if node:
                record.description = node.text.strip()

            ##########
            # visits #
            ##########
            node = soup.select_one('div[class*="visitCounter-"]>span')
            if node:
                record.visits = node.text.strip()

            #################
            # other details #
            #################
            node = soup.select(
                'div[class*="gradientScrollWrapper"]>div>div>div>ul')
            if node:
                for item in node:
                    try:
                        liNodes = item.select('li')
                        for li in liNodes:
                            try:
                                check = li.select_one('div')
                                if check:
                                    h4Node = li.select_one('h4')
                                    if h4Node:
                                        h4 = h4Node.text.strip()
                                        subLi = check.select('ul>li')

                                        if h4.lower().__eq__('utilities included'):
                                            record.utilitiesIncluded = extractUtilitiesIncluded(
                                                subLi, check)
                                        elif h4.lower().__eq__('wi-fi and more'):
                                            record.wifiAndMore = extractBackUp(
                                                check)
                                        elif h4.lower().__eq__('appliances'):
                                            record.appliances = extractOtherDetails(
                                                subLi, check)
                                        elif h4.lower().__eq__('personal outdoor space'):
                                            record.personalOutdoorSpace = extractBackUp(
                                                check)
                                        elif h4.lower().__eq__('amenities'):
                                            record.amenities = extractOtherDetails(
                                                subLi, check)
                                        elif h4.lower().__eq__('elevator accessibility features'):
                                            record.elevatorAccessibilityFeatures = extractOtherDetails(
                                                subLi, check)
                                        else:
                                            log("some new field might have been discovered.")
                                else:
                                    check = li.select_one('dl')
                                    if check:
                                        dtNode = check.select_one('dt')
                                        ddNode = check.select_one('dd')
                                        if dtNode and ddNode:

                                            dtText = dtNode.text.strip().lower()
                                            ddText = ddNode.text.strip()

                                            if dtText.__eq__('parking included'):
                                                record.parkingIncluded = ddText
                                            elif dtText.__eq__('agreement type'):
                                                record.agreementType = ddText
                                            elif dtText.__eq__('move-in date'):
                                                record.moveInDate = ddText
                                            elif dtText.__eq__('pet friendly'):
                                                record.petFriendly = ddText
                                            elif dtText.startswith('size'):
                                                if ddText.lower().strip().__eq__('not available'):
                                                    record.size = ddText
                                                else:
                                                    record.size = ddText+" " + \
                                                        dtText.replace(
                                                            'size', '').strip()
                                            elif dtText.__eq__('furnished'):
                                                record.furnished = ddText
                                            elif dtText.__eq__('air conditioning'):
                                                record.airConditioning = ddText
                                            elif dtText.__eq__('smoking permitted'):
                                                record.smokingPermitted = ddText
                                            elif dtText.__eq__('barrier-free entrances and ramps'):
                                                record.barrierFreeEntrancesAndRamps = ddText
                                            elif dtText.__eq__('visual aids'):
                                                record.visualAids = ddText
                                            elif dtText.__eq__('accessible washrooms in suite'):
                                                record.accessibleWashroomsInSuite = ddText
                                            else:
                                                log("some new field might have been discovered.")
                            except Exception as ex:
                                log("liNodes(other details)=> " + str(ex))
                    except Exception as ex:
                        log("Other details()=> " + str(ex))

            results.append(record)
            index = results.index(record)
            index += 1

            log(str(index) + "- | " + record.title + " | " + record.price)
        else:
            log("cant find title")
    except Exception as ex:
        log("scrapeAd()=> " + str(ex))


def extractUtilitiesIncluded(subLi, check):
    value = ""

    if subLi:
        for util in subLi:
            try:
                temp = util.select_one('svg')
                if temp:
                    raw = temp['aria-label'].strip()
                    if raw.lower().startswith('yes'):
                        if value.__eq__(""):
                            value = raw.split(':')[1].strip()
                        else:
                            value += ", " + raw.split(':')[1].strip()
            except:
                pass
    else:
        value = extractBackUp(check)

    return value


def extractOtherDetails(subLi, check):
    value = ""

    if subLi:
        for util in subLi:
            try:
                readText = util.text.strip()
                if not readText.__eq__(""):
                    if value.__eq__(""):
                        value = readText
                    else:
                        value += ", " + readText
            except:
                pass
    else:
        value = extractBackUp(check)

    return value


def extractBackUp(check):
    value = ''

    backup = check.select_one('ul')
    if backup:
        value = backup.text.strip()

    return value


def exportToCSV(name):
    name = "Results/" + name + ".csv"
    log("Exporting collected results to CSV file.")
    log("File name => " + name)

    try:
        with open(name, "a", newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csvColumns)
            writer.writeheader()

            for d in results:
                data = {'Province': d.province, 'Date Scraped': d.dateScraped, 'Date Posted': d.datePosted, 'Title': d.title, 'Address': d.address,
                        'Price': d.price, 'Type': d.type, 'Bedrooms': d.bedrooms, 'Bathrooms': d.bathrooms, 'Description': d.description, 'Utilities Included': d.utilitiesIncluded, 'Wi-Fi And More': d.wifiAndMore, 'Parking Included': d.parkingIncluded, 'Agreement Type': d.agreementType, 'Move-In Date': d.moveInDate, 'Pet Friendly': d.petFriendly, 'Size': d.size, 'Furnished': d.furnished, 'Appliances': d.appliances, 'Air Conditioning': d.airConditioning, 'Personal Outdoor Space': d.personalOutdoorSpace, 'Smoking Permitted': d.smokingPermitted, 'Amenties': d.amenities, 'Elevator Accessibility Features': d.elevatorAccessibilityFeatures, 'Barrier-free Entrances and Ramps': d.barrierFreeEntrancesAndRamps, 'Visual Aids': d.visualAids, 'Accessible Washrooms in Suite': d.accessibleWashroomsInSuite, 'Visits': d.visits, 'Kijiji Url': d.kijijiURL}
                writer.writerow(data)
    except Exception as ex:
        log("exportToCSV()=> "+str(ex))


startScraping()

# input("ALL CAUGHT UP... Press any key to continue.")
