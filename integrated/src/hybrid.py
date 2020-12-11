import common_functions as common
import json
import sys
import time


SLEEPING_SECONDS = 8


if __name__ == '__main__':
    '''
    while True:
        read_atomic_level_requests()
        read_from_kafka_and_run_a_single_atomic_level_request()
    '''

    # for j in range(15):
        # atomic_level_request = common.read_next_atomic_level_from_kafka()
        # print(j, atomic_level_request)
        # print()
    # exit(0)
    
    if '-j' in sys.argv:
        atomic_level_request = common.read_next_atomic_level_from_kafka()
        # atomic_level_request = json.loads([ l.strip() for l in open('../data/my_atomic_instance.txt', 'rt') if l.strip() ][0])
        # atomic_level_request = None
        print(json.dumps(json.loads(atomic_level_request)[:-1]))

        if atomic_level_request is not None:
            common.crawl_atomic(atomic_level_request)
    elif '--empty' in sys.argv:
        atomic_level_request = ''
        while atomic_level_request is not None:
            atomic_level_request = common.read_next_atomic_level_from_kafka()
            print(atomic_level_request)
    elif '--youtube' in sys.argv:
        '''
        ["youtube", "id_videos_youtube", "PcmLdV2NXww", "video", {"data_min": "2020-09-20 10:00", "pasta_da_saida": "data", "coletor": "youtube", "id_videos_youtube": ["PcmLdV2NXww"], "palavras": ["covid-19"], "id_canais_youtube": ["MinSaudeBR", "https://www.youtube.com/channel/UC12zKGLhMhDeDidoctM6BrA"], "chaves_de_acesso": [{"token_acesso": "ENCIsswxqUsEqYDNLAUqyRYUimKSetOp"}]}]
        ["youtube", "id_canais_youtube", "https://www.youtube.com/channel/UC12zKGLhMhDeDidoctM6BrA", "channel", {"data_min": "2020-09-20 10:00", "pasta_da_saida": "data", "coletor": "youtube", "id_videos_youtube": ["PcmLdV2NXww"], "palavras": ["covid-19"], "id_canais_youtube": ["MinSaudeBR", "https://www.youtube.com/channel/UC12zKGLhMhDeDidoctM6BrA"], "chaves_de_acesso": [{"token_acesso": "ENCIsswxqUsEqYDNLAUqyRYUimKSetOp"}]}]
        ["youtube", "palavras", "covid-19", "keyword", {"data_min": "2020-09-20 10:00", "pasta_da_saida": "data", "coletor": "youtube", "id_videos_youtube": ["PcmLdV2NXww"], "palavras": ["covid-19"], "id_canais_youtube": ["MinSaudeBR", "https://www.youtube.com/channel/UC12zKGLhMhDeDidoctM6BrA"], "chaves_de_acesso": [{"token_acesso": "ENCIsswxqUsEqYDNLAUqyRYUimKSetOp"}]}]
        '''
        atomic_level_request = common.decrypt_string('["youtube", "id_canais_youtube", "MinSaudeBR", "channel", {"data_min": "2020-11-20 10:00", "pasta_da_saida": "data", "coletor": "youtube", "id_videos_youtube": ["PcmLdV2NXww"], "palavras": ["covid-19"], "id_canais_youtube": ["MinSaudeBR", "https://www.youtube.com/channel/UC12zKGLhMhDeDidoctM6BrA"], "chaves_de_acesso": [{"token_acesso": "ENCIsswxqUsEqYDNLAUqyRYUimKSetOp"}]}]')
        print(json.dumps(json.loads(atomic_level_request)[:-1]))
        common.crawl_atomic(atomic_level_request)
        exit(0)
    else:
        while True:
            atomic_level_request = common.read_next_atomic_level_from_kafka()
            if atomic_level_request is not None:
                try:
                    common.crawl_atomic(atomic_level_request)
                except Exception as e:
                    print('crawling failed: %s' % str(e))
            
            time.sleep(SLEEPING_SECONDS)
