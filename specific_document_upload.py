# -*- coding: utf-8 -*-
"""
Uploads a specific decison's text and embeddings into BigQuery tables.
"""

import requests
from bs4 import BeautifulSoup
import re
import logging
from scrapper import make_request, insert_data
from embedder import get_embeddings
from typing import List
import argparse

# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Uploads a specific decison''s text and embeddings into BigQuery tables.')

# Logging settings
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/specific_doc_upload.log', 'w', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class DataConst:

    def __init__(self, mahkeme_turu, esasNo, esasYil, kararYil, kararNo):
        self.arananKelime = ""
        self.esasYil = esasYil
        self.esasIlkSiraNo = esasNo
        self.esasSonSiraNo = esasNo
        self.kararYil = kararYil
        self.kararIlkSiraNo = kararNo
        self.kararSonSiraNo = kararNo
        self.baslangicTarihi = ""
        self.bitisTarihi = ""
        self.siralama = "1"
        self.siralamaDirection = "desc"
        self.birimYrgKurulDaire = ""
        self.birimYrgHukukDaire = ""
        self.birimYrgCezaDaire = ""
        self.pageSize = 10 # type: ignore
        self.pageNumber = 1 # type: ignore

        if 'kurulu' in mahkeme_turu.lower():
            self.yargitayMah = mahkeme_turu
            self.birimYrgKurulDaire = mahkeme_turu
        elif 'hukuk' in mahkeme_turu.lower() and 'daire' in mahkeme_turu.lower():
            self.hukuk = mahkeme_turu
            self.birimYrgHukukDaire = mahkeme_turu
        elif 'ceza' in mahkeme_turu.lower() and 'daire' in mahkeme_turu.lower():
            self.ceza = mahkeme_turu
            self.birimYrgCezaDaire = mahkeme_turu
        else:
            print('Invalid court type')

        def get_attrs():

[FILL IN]

# Find and get the related document from Yargitay
def get_document(mahkeme_turu, esasNo, esasYil, kararYil, kararNo) -> List:
    """
    This function finds and gets the related document from Yargitay.

    Args:
        mahkeme_turu (str): The type of court.
        daire (str): The number of the court.
        esasNo (str): The number of the case.
        esasYil (str): The year of the case.
        kararYil (str): The year of the decision.
        kararNo (str): The number of the decision.

    Returns:
        str: The text of the decision.
    """
    try:
        # Construct initial request data
        initial_request_data = {"data": {
            "arananKelime": "",
        }}

        if 'kurulu' in mahkeme_turu.lower():
            initial_request_data['data']['yargitayMah'] = mahkeme_turu
            initial_request_data['data']['birimYrgKurulDaire'] = mahkeme_turu
        elif 'hukuk' in mahkeme_turu.lower() and 'daire' in mahkeme_turu.lower():
            initial_request_data['data']['hukuk'] = mahkeme_turu
            initial_request_data['data']['birimYrgHukukDaire'] = mahkeme_turu
        elif 'ceza' in mahkeme_turu.lower() and 'daire' in mahkeme_turu.lower():
            initial_request_data['data']['ceza'] = mahkeme_turu
            initial_request_data['data']['birimYrgCezaDaire'] = mahkeme_turu
        else:
            print('Invalid court type')

        initial_request_data['data']["esasYil"] = esasYil
        initial_request_data['data']["esasIlkSiraNo"] = esasNo
        initial_request_data['data']["esasSonSiraNo"] = esasNo
        initial_request_data['data']["kararYil"] = kararYil
        initial_request_data['data']["kararIlkSiraNo"] = kararNo
        initial_request_data['data']["kararSonSiraNo"] = kararNo
        initial_request_data['data']["baslangicTarihi"] = ""
        initial_request_data['data']["bitisTarihi"] = ""
        initial_request_data['data']["siralama"] = "1"
        initial_request_data['data']["siralamaDirection"] = "desc"
        initial_request_data['data']["birimYrgKurulDaire"] = ""
        initial_request_data['data']["birimYrgHukukDaire"] = ""
        initial_request_data['data']["birimYrgCezaDaire"] = ""
        initial_request_data['data']["pageSize"] = 10 # type: ignore
        initial_request_data['data']["pageNumber"] = 1 # type: ignore

        # Initial request
        headers = {
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.2747.111 Safari/537.36',
            "Referer": "https://karararama.yargitay.gov.tr/",
        }
        page = requests.post(
            url='https://karararama.yargitay.gov.tr/aramalist',
            headers=headers,
            json=initial_request_data
        )
        cookies = page.cookies
        documents_list = page.json()['data']['data']
        data = []
        if len(documents_list) > 1:
            logger.warning(f'Multiple documents found for {mahkeme_turu} {esasYil, esasNo, kararYil, kararNo}, -- Processing both')
        for doc in documents_list:
                # Construct metadata
                doc_id = doc['id']
                daire_type = re.sub(r'[^a-zA-Z\ ]','', doc['daire']).lower().replace('dairesi', '').strip()
                daire_no = re.sub(r'[^1-9]','', doc['daire']).strip()
                # Format change 2013/12234 -> 2013 12234
                esas_yili = doc['esasNo'][:4]
                esas_no = doc['esasNo'][5:]
                karar_yili = doc['kararNo'][:4]
                karar_no = doc['kararNo'][5:]
                # Format change: 13.11.2014 -> 2014-11-13
                karar_tarihi = '-'.join(doc['kararTarihi'].split('.')[::-1])
                # Make GET request to get the document text
                content = make_request(url=f'https://karararama.yargitay.gov.tr/getDokuman?id={doc["id"]}', headers=headers, cookies=cookies)
                soup = BeautifulSoup(content, 'html.parser')
                text = soup.body.get_text(separator='\
')

                # Construct response row
                row = {
                    'doc_id':doc_id,
                    'daire':daire_type,
                    'daire_no':daire_no,
                    'esas_yili':esas_yili,
                    'esas_no':esas_no,
                    'karar_yili':karar_yili,
                    'karar_no':karar_no,
                    'karar_tarihi':karar_tarihi,
                    'metin':text
                    }
                data.append(row)

        return data

    except Exception as e:
            logger.error(e)

def list_documenst(html_file_address: str) -> List:
    """
    This function lists all the documents in the hmtl file.

    Returns:
        List: A list of all the documents in the database.
    """
    with open(html_file_address, 'r') as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    target_divs = soup.find_all(
        'div',
        class_="flex select-text space-x-0 w-full h-max text-left items-center tracking-wider font-bold text-dejureOrange md:space-x-2 text-sm md:text-base")

    return set([div.get_text(strip=True) for div in target_divs])

def main(html_file_address):

    # # Add arguments to the parser
    # parser.add_argument('--html_file_address', type=str, help='full path of the html file containing the list of documents')

    # # Parse the command-line arguments
    # args = parser.parse_args()

    # # Get the information from the user
    # html_file_address = args.html_file_address


    # Get the list of documents from the html file
    documents = list_documenst(html_file_address)

    # Contruct info and make request for each document
    for document in documents:
        mahkeme_turu = document.split(',')[0][8:].strip()
        esasYil, esasNo = document.split()[5].split('/')
        kararYil, kararNo = document.split()[7].split('/')

        # Make request
        text_data = get_document(mahkeme_turu, esasNo, esasYil, kararYil, kararNo)
        insert_data(text_data, table='yargitay_kararlari')

        embeddings_data = []
        for row in text_data:
            embeddings = get_embeddings(row['text'])
            embeddings_data.append(embeddings)

        insert_data(embeddings_data, table='embeddings')

if __name__ == '__main__':
    main("/Users/tevfikcagridural/Library/CloudStorage/GoogleDrive-c.dural@gmail.com/My Drive/Projects/yargitay_arama_motoru/arkadan_carpma.html")