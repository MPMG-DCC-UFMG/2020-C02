class YoutubeCrawlerAPI():
  def get_video_statistics(self, youtube, video_id):
    video_analytics_info = youtube.videos().list(
        id=video_id,
        part="statistics",
        maxResults=50
    ).execute()

    result_dict = {}
    if("likeCount" in video_analytics_info['items'][0]['statistics']):
      result_dict['total_likes'] = video_analytics_info['items'][0]['statistics']['likeCount']
      result_dict['total_dislikes'] = video_analytics_info['items'][0]['statistics']['dislikeCount']

    if('viewCount' in video_analytics_info['items'][0]['statistics']):
      result_dict['visualizacoes'] = video_analytics_info['items'][0]['statistics']['viewCount']
    else:
      result_dict['visualizacoes'] = 0

    return result_dict

  def get_channel_videos(self, youtube, channel_id):
    final_response_dict = {}
    channel_profile = {}
    videos_details = {}

    # Channel information
    request = youtube.channels().list(
            part='statistics',
            id=channel_id
        )
    subscriber_count = (request.execute())['items'][0]['statistics']['subscriberCount']

    request = youtube.channels().list(
            part='snippet',
            id=channel_id
        )
    channel_name = (request.execute())['items'][0]['snippet']['title']

    channel_profile['identificador'] = channel_id
    channel_profile['nome_canal'] = channel_name
    channel_profile['numero_inscritos'] = subscriber_count

    # Videos information
    channels_response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    for channel in channels_response['items']:
        uploads_list_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]
        playlistitems_list_request = youtube.playlistItems().list(
            playlistId=uploads_list_id,
            part="snippet",
            maxResults=50
          )
        while playlistitems_list_request:
            playlistitems_list_response = playlistitems_list_request.execute()
            for playlist_item in playlistitems_list_response["items"]:
                video_id = playlist_item["snippet"]["resourceId"]["videoId"]

                title = playlist_item["snippet"]["title"]
                description = playlist_item["snippet"]["description"]
                published_at = playlist_item["snippet"]["publishedAt"]
        
                video_info = {}
                video_info['identificador'] = video_id
                video_info['titulo'] = title
                video_info['descricao'] = description
                video_info['data_publicacao'] = published_at
                video_info['link_video'] = ('https://www.youtube.com/watch?v=' + video_id)

                video_statistics = self.get_video_statistics(youtube, video_id)

                video_info['estatisticas'] = video_statistics
                videos_details[video_id] = video_info

            playlistitems_list_request = youtube.playlistItems().list_next(
                playlistitems_list_request, playlistitems_list_response
            )

    final_response_dict['informacao_canal'] = channel_profile
    final_response_dict['informacao_videos'] = videos_details

    return final_response_dict

  def get_videos_by_keyword(self, youtube, keyword):
    next_video_request = youtube.search().list(
            part='snippet',
            type='video',
            regionCode='BR',
            q=keyword,
            maxResults=50
        )
    
    videos_details = {}
    while next_video_request:
      next_video_request_response = next_video_request.execute()
      video_results = next_video_request_response["items"]

      for single_video in video_results:
        video_id = single_video["id"]["videoId"]
        title = single_video["snippet"]["title"]
        description = single_video["snippet"]["description"]
        published_at = single_video["snippet"]["publishedAt"]

        video_info = {}
        video_info['identificador'] = video_id
        video_info['titulo'] = title
        video_info['descricao'] = description
        video_info['data_publicacao'] = published_at
        video_info['link_video'] = ('https://www.youtube.com/watch?v=' + video_id)

        video_statistics = self.get_video_statistics(youtube, video_id)
        video_info['estatisticas'] = video_statistics
        videos_details[video_id] = video_info

      next_video_request = youtube.search().list_next(
          next_video_request, next_video_request_response
      )

    return videos_details

  def get_video_comments(self, youtube, video_id, max_comments):
    next_video_request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50
        )
    
    comments = {}
    still_collecting = True
    while(next_video_request and still_collecting):
      next_video_request_response = next_video_request.execute()
      video_results = next_video_request_response["items"]

      for single_comment in video_results:
        comment_id = single_comment["id"]

        if(single_comment["snippet"]['topLevelComment']):
          text = single_comment["snippet"]['topLevelComment']["snippet"]["textDisplay"]
          published_at = single_comment["snippet"]['topLevelComment']["snippet"]["publishedAt"]
          author_name = single_comment["snippet"]['topLevelComment']["snippet"]["authorDisplayName"]
          author_id = single_comment["snippet"]['topLevelComment']["snippet"]["authorChannelId"]['value']
          like_count = single_comment["snippet"]['topLevelComment']["snippet"]["likeCount"]
        else:
          text = single_comment["snippet"]["textDisplay"]
          published_at = single_comment["snippet"]["publishedAt"]
          author_id = single_comment["snippet"]["authorChannelId"]['value']
          author_name = single_comment["snippet"]["authorDisplayName"]
          like_count = single_comment["snippet"]["likeCount"]

        comment_info = {}
        comment_info['id_video'] = video_id
        comment_info['id_comentario'] = comment_id
        comment_info['texto'] = text
        comment_info['id_autor'] = author_id
        comment_info['nome_autor'] = author_name
        comment_info['numero_likes'] = like_count
        comment_info['data_publicacao'] = published_at

        comments[comment_id] = comment_info
      
      if(len(comments) >= max_comments):
        still_collecting = False
      
      next_video_request = youtube.commentThreads().list_next(
          next_video_request, next_video_request_response
      )

    return comments