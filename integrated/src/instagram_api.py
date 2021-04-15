#!/usr/bin/python

import sys
import os
from datetime import datetime
import json


from instagram_coletor import Coletor
import local_instaloader.instaloader as localinstaloader


def needs_credential(js):
    coletor = Coletor(input_json=js)

    collection_types = []

    if len(coletor.user_list) > 0:
        collection_types.append("perfil")
    if len(coletor.hashtag_list) > 0:
        collection_types.append("hashtag")

    # print(coletor.user_list, "\n", coletor.hashtag_list)
    # print(collection_types)

    return "hashtag" in collection_types

def get_coletor_object(js):
    coletor = Coletor(input_json=js)

    return coletor


def authenticate(js):
    coletor = Coletor(input_json=js)
    instaloaderInstance = None
    ### Verifica se login valido
    try:
        proxy_info = coletor.get_proxy(does_not_increment=True)
        instaloaderInstance = localinstaloader.Instaloader(proxies=proxy_info)
        instaloaderInstance.login(user=coletor.instagram_user, passwd=coletor.instagram_passwd)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('\nErro: ', e, '\tDetalhes: ', exc_type, fname, exc_tb.tb_lineno, '\tData e hora: ',
              datetime.now(),
              flush=True)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_document = coletor.getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)

        coletor.create_error_file(filename_output=coletor.filename_unified_data_file,
                                 error_document=error_document)
        #print("Finalizando script.")
        #sys.exit(1)

    return instaloaderInstance
