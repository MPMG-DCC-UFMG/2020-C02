# -*- coding: utf-8 -*-

import crawler.api
import dateutil.parser
import os
import time
import json
from sys import argv

from googleapiclient.discovery import build

def main(input_json_folder):
  api = crawler.api.YoutubeCrawlerAPI()
  api_keys = []
  api_key_usage = -1
  still_collecting = True

  data = {}
  
  if '-d' in argv:
    idx = argv.index('-d') + 1
    data = json.loads(argv[idx])
  else:
    input_json = open(input_json_folder, "r") 
    data = json.loads(input_json.read()) 
    input_json.close() 

  # print(input_json)

  for api_key in data['chaves_de_acesso']:
    api_keys.append(api_key['token_acesso'])

  channels = data['id_canais_youtube']
  videos = data['id_videos_youtube']
  keywords = data['palavras']
  max_comments = int(data['max_comentarios']) if 'max_comentarios' in data else None
  data_min = dateutil.parser.parse(data['data_min']) if 'data_min' in data else None
  data_max = dateutil.parser.parse(data['data_max']) if 'data_max' in data else None

  youtube = build('youtube', 'v3', developerKey=api_keys[api_key_usage])

  channels_info = []
  for channel in channels:
    still_collecting = True
    while(api_key_usage != len(api_keys) and still_collecting):
      try:
        channels_info.append(api.get_channel_videos(youtube, channel))
        still_collecting = False
      except:
        api_key_usage += 1
        if(api_key_usage < (len(api_keys) - 1)):
          youtube = build('youtube', 'v3', developerKey=api_keys[api_key_usage])

  videos_info = []
  api_key_usage = -1
  still_collecting = True
  for video in videos:
    still_collecting = True
    while(api_key_usage != len(api_keys) and still_collecting):
      try:
        videos_info.append(api.get_video_comments(youtube, video, max_comments, data_min, data_max))
        still_collecting = False
      except Exception as e:
        api_key_usage += 1
        if(api_key_usage < (len(api_keys) - 1)):
          youtube = build('youtube', 'v3', developerKey=api_keys[api_key_usage])
        print('problema na coleta: %s' % str(e))

  keywords_info = []
  api_key_usage = -1
  still_collecting = True
  for keyword in keywords:
    still_collecting = True
    while(api_key_usage != len(api_keys) and still_collecting):
      try:
        keywords_info.append(api.get_videos_by_keyword(youtube, keyword))
        still_collecting = False
      except:
        api_key_usage += 1
        if(api_key_usage < (len(api_keys) - 1)):
          youtube = build('youtube', 'v3', developerKey=api_keys[api_key_usage])

  if(len(keywords) == 0): still_collecting = False
  final_dict = dict()
  final_dict['informacoes_canal_youtube'] = channels_info
  final_dict['comentarios_video_youtube'] = videos_info
  final_dict['videos_por_keyword_youtube'] = keywords_info

  if(still_collecting): print("\n\nAVISO!\n\nA quantidade diária de coletas permitida nas API Keys informadas foi atingida, portanto alguns dados não puderam ser coletados. Volte amanhã ou insira novas API Keys para coletar mais dados.\n\n")
  else:print("\n\nDados salvos com sucesso!\n\n")

  output_file = os.path.join(data['pasta_da_saida'], (str(int(time.time()*1000)) + '.json'))
  with open(output_file, 'w') as outfile:
      json.dump(final_dict, outfile)

input_json_folder = argv[1]
main(argv[1])
