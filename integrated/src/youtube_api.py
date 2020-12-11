import common_functions as common
import copy
import datetime
import dateutil.parser
import json
import pytz
import time
import tzlocal
import urllib

from googleapiclient.discovery import build as youtube_build
from tqdm import tqdm


TOPIC_KAFKA_YOUTUBE_COMMENT = 'crawler_youtube_comentario'
TOPIC_KAFKA_YOUTUBE_PROFILE = 'crawler_youtube_perfil'
TOPIC_KAFKA_YOUTUBE_VIDEO = 'crawler_youtube_post'
memoize_channels = {}


def credentials_to_api_object(tokens):
    this_key = tokens['token_acesso']
    this_api = None

    try:
        this_api = youtube_build('youtube', 'v3', developerKey=this_key)
    except:
        pass

    return this_api


def username2channels(username, youtube):
    # channels # forUsername

    request = youtube.this_api().channels().list(
        part='id',
        forUsername=username
    )

    items = []
    try:
        items = request.execute()['items']
    except:
        pass

    return [ channel_info['id'] for channel_info in items ]


def utc2local(utc_time):
    '''
    Thanks to https://stackoverflow.com/a/32904812 for the solution
    '''
    local_timezone = tzlocal.get_localzone() # get pytz tzinfo
    # print(local_timezone)
    return utc_time.replace(tzinfo=pytz.utc).astimezone(local_timezone)


def make_unique_list(mlist):
    answer = []
    already = set()

    for element in mlist:
        if element not in already:
            answer.append(element)
            already.add(element)

    return answer
    

def link_to_id(url):
    url = str(url)

    is_url = 'http:' in url or 'https:' in url or 'www.' in url or 'youtube.com' in url or 'youtu.be' in url
    if not is_url:
        return url


    if 'youtube.com/watch?' in url and 'v=' in url:
        get_dict = {}
        try:
            get_dict = dict(urllib.parse.parse_qs(urllib.parse.urlsplit(url).query))
        except:
            pass

        return get_dict['v'][0] if 'v' in get_dict else url


    if '/channel/' in url or 'youtu.be/' in url:
        url_without_get = url if '?' not in url else url[:url.find('?')]
        return url_without_get.split('/')[-1]


    return url


class YoutubeCrawlerAPI():
    def __init__(self, data):
        self.apis = []
        # self.this_api = None

        for api_key in data['chaves_de_acesso']:
            this_key = api_key['token_acesso']
            try:
                self.apis.append(youtube_build('youtube', 'v3', developerKey=this_key))
                # self.api_keys.append(this_key)
            except:
                continue

        self.api_index = (len(self.apis)-1) if self.apis else None
        # self.this_api = self.apis[self.api_index] if self.api_index is not None else None

        self.max_comments, self.data_min, self.data_max = None, None, None
        try:
            self.max_comments = int(data['max_comentarios']) if 'max_comentarios' in data else None
            self.data_min = dateutil.parser.parse(data['data_min']) if 'data_min' in data else None
            self.data_max = dateutil.parser.parse(data['data_max']) if 'data_max' in data else None
        except Exception as e:
            raise Exception('cannot initialize YouTube crawler due to: %s' % str(e))

        return


    def this_api(self):
        self.api_index = (self.api_index + 1) % len(self.apis)
        # print('api_index == ' + str(self.api_index))
        return self.apis[self.api_index]


    def get_video_statistics(self, video_id):
        video_analytics_info = self.this_api().videos().list(
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

    def get_channel_videos(self, channel_id, min_dt=None):
        global TOPIC_KAFKA_YOUTUBE_PROFILE
        global TOPIC_KAFKA_YOUTUBE_VIDEO
        global memoize_channels

        final_response_dict = {}
        channel_profile = {}
        videos_details = {}

        channel_key = copy.deepcopy(channel_id)
        if '/' in channel_id:
            channel_id = channel_id.split('/')[-1]
            channel_key = channel_key.replace('/', ': ')

        if channel_id not in memoize_channels:
            kafka_prod = common.connect_kafka_producer()

            # Channel information
            request = self.this_api().channels().list(
                part='statistics',
                id=channel_id
            )
            result = request.execute()

            if 'items' not in result:
                raise Exception("Error 404: channel id \"%s\" not found" % channel_id)

            if(min_dt is not None):
                min_dt = (min_dt - datetime.timedelta(minutes=6)).replace(tzinfo=tzlocal.get_localzone()).astimezone(tzlocal.get_localzone())

            subscriber_count = result['items'][0]['statistics']['subscriberCount']

            request = self.this_api().channels().list(
                part='snippet',
                id=channel_id
            )
            channel_name = (request.execute())['items'][0]['snippet']['title']

            channel_profile['identificador'] = channel_id
            channel_profile['nome_canal'] = channel_name
            channel_profile['numero_inscritos'] = subscriber_count

            common.publish_kafka_message(kafka_prod, TOPIC_KAFKA_YOUTUBE_PROFILE, self.crawling_id, json.dumps(channel_profile))

            # Videos information
            channels_response = self.this_api().channels().list(part='contentDetails', id=channel_id).execute()

            for channel in channels_response['items']:
                uploads_list_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]
                playlistitems_list_request = self.this_api().playlistItems().list(
                    playlistId=uploads_list_id,
                    part="snippet",
                    maxResults=50
                )

                bar = None
                while playlistitems_list_request:
                    playlistitems_list_response = playlistitems_list_request.execute()

                    if bar is None:
                        bar = tqdm(total=int(playlistitems_list_response['pageInfo']['totalResults']), ascii=True)
                    published_at_dts = []

                    for playlist_item in playlistitems_list_response["items"]:
                        video_id = playlist_item["snippet"]["resourceId"]["videoId"]

                        title = playlist_item["snippet"]["title"]
                        description = playlist_item["snippet"]["description"]
                        published_at = playlist_item["snippet"]["publishedAt"]
                        published_at_dt = utc2local(dateutil.parser.parse(published_at))
                        published_at_dts.append(published_at_dt)
                        # bar.write(str(published_at_dt))
                    
                        video_info = {}
                        video_info['identificador'] = video_id
                        video_info['titulo'] = title
                        video_info['descricao'] = description
                        video_info['texto'] = str(title) + ' - ' + str(description)
                        video_info['data_publicacao'] = published_at
                        video_info['link_video'] = ('https://www.youtube.com/watch?v=' + video_id)

                        video_statistics = self.get_video_statistics(video_id)

                        video_info['estatisticas'] = video_statistics

                        if min_dt is None or published_at_dt >= min_dt:
                            common.publish_kafka_message(kafka_prod, TOPIC_KAFKA_YOUTUBE_VIDEO, self.crawling_id, json.dumps(video_info))
                            # videos_details[video_id] = video_info

                        bar.update(1)
                
                    if min_dt is not None and all([ published_at_dt < min_dt for published_at_dt in published_at_dts ]):
                        break

                    playlistitems_list_request = self.this_api().playlistItems().list_next(
                        playlistitems_list_request, playlistitems_list_response
                    )

                bar.close()

            # final_response_dict['informacao_canal'] = channel_profile
            # final_response_dict['informacao_videos'] = videos_details
        
            memoize_channels[channel_id] = final_response_dict

        return
        # return {channel_key: memoize_channels[channel_id]}


    def get_videos_by_keyword(self, keyword, max_results=50):
        global TOPIC_KAFKA_YOUTUBE_VIDEO

        kafka_prod = common.connect_kafka_producer()

        next_video_request = self.this_api().search().list(
            part='snippet',
            type='video',
            regionCode='BR',
            q=keyword,
            maxResults=max_results
        )

        total_videos_details = 0
        # videos_details = {}
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
                video_info['texto'] = str(title) + ' - ' + str(description)
                video_info['data_publicacao'] = published_at
                video_info['link_video'] = ('https://www.youtube.com/watch?v=' + video_id)

                video_statistics = self.get_video_statistics(video_id)
                video_info['estatisticas'] = video_statistics

                common.publish_kafka_message(kafka_prod, TOPIC_KAFKA_YOUTUBE_VIDEO, self.crawling_id, json.dumps(video_info))
                total_videos_details += 1
                # videos_details[video_id] = video_info

            if total_videos_details >= max_results:
                break

            next_video_request = self.this_api().search().list_next(
                next_video_request, next_video_request_response
            )

        return
        # return videos_details

    def get_video_comments(self, video_id, max_comments, min_dt, max_dt):
        global TOPIC_KAFKA_YOUTUBE_COMMENT
        # print(youtube.captions().download(id=video_id).execute())

        kafka_prod = common.connect_kafka_producer()

        next_video_request = self.this_api().commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50,
            order='time'
            # order='relevance' # order='time'
        )

        if(min_dt is not None):
            min_dt = (min_dt - datetime.timedelta(minutes=6)).replace(tzinfo=tzlocal.get_localzone()).astimezone(tzlocal.get_localzone())
        if(max_dt is not None):
            max_dt = (max_dt - datetime.timedelta(minutes=6)).replace(tzinfo=tzlocal.get_localzone()).astimezone(tzlocal.get_localzone())

        total_comments = 0
        # comments = {}
        oldest_published_dt = datetime.datetime.now() + datetime.timedelta(days=360)
        still_collecting = True
        while(next_video_request and still_collecting):
            next_video_request_response = next_video_request.execute()

            # del next_video_request_response['items']
            # print(next_video_request_response)

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
                published_at_dt = utc2local(dateutil.parser.parse(published_at))

                comment_info = {}
                comment_info['id_video'] = video_id
                comment_info['id_comentario'] = comment_id
                comment_info['identificador'] = comment_id
                comment_info['texto'] = text
                comment_info['id_autor'] = author_id
                comment_info['nome_autor'] = author_name
                comment_info['numero_likes'] = like_count
                comment_info['data_publicacao'] = published_at

                oldest_published_dt = copy.deepcopy(published_at_dt)

                sat_min_date = min_dt is None or published_at_dt >= min_dt
                sat_max_date = max_dt is None or published_at_dt < max_dt
                sat_max_comments = max_comments is None or total_comments < max_comments
                # print(published_at_dt, min_dt, sat_min_date)
                if sat_min_date and sat_max_date and sat_max_comments:
                    common.publish_kafka_message(kafka_prod, TOPIC_KAFKA_YOUTUBE_COMMENT, self.crawling_id, json.dumps(comment_info))
                    total_comments += 1
                    # print('>> added')

                # print(published_at)
                # time.sleep(0.25)
                # print(comment_info)
            
            if(max_comments is not None and total_comments >= max_comments):
                still_collecting = False
            elif(min_dt is not None and oldest_published_dt < min_dt):
                still_collecting = False
            
            next_video_request = self.this_api().commentThreads().list_next(
                next_video_request, next_video_request_response
            )

        return
        

    def download_single_channel(self, channel_name, crawling_id):
        profile_channels = username2channels(channel_name, self)
        channels = [ channel_name ] if not profile_channels else [ ('%s/%s' % (channel_name, channel_id)) for channel_id in profile_channels ]
        self.crawling_id = copy.deepcopy(crawling_id)

        for channel in channels:
            still_collecting = True
            while(still_collecting):
                try:
                    self.get_channel_videos(channel, self.data_min)
                    still_collecting = False
                except Exception as e:
                    if 'error 404' in str(e).lower():
                        # channels_info[channel] = { ERROR_KEY: 'canal não encontrado' }
                        still_collecting = False
                    else:
                        print('problema na coleta: %s\nWaiting 60 seconds...' % str(e))
                        time.sleep(60)

        return
    
    
    def download_single_video(self, video_id, crawling_id):
        video = link_to_id(video_id)
        self.crawling_id = copy.deepcopy(crawling_id)
        self.get_video_comments(video, self.max_comments, self.data_min, self.data_max)
        exit(0)

        still_collecting = True
        while(still_collecting):
            try:
                self.get_video_comments(video, self.max_comments, self.data_min, self.data_max)
                still_collecting = False
            except Exception as e:
                if 'error 404' in str(e).lower():
                    videos_info[video] = { ERROR_KEY: 'video não encontrado' }
                    still_collecting = False
                else:
                    print('problema na coleta: %s\nWaiting 60 seconds...' % str(e))
                    time.sleep(60)

        return

    def download_single_word(self, keyword, crawling_id):
        self.crawling_id = copy.deepcopy(crawling_id)

        still_collecting = True
        while(still_collecting):
            try:
                self.get_videos_by_keyword(keyword) if keyword.strip() else {}
                still_collecting = False
            except Exception as e:
                print('problema na coleta: %s\nWaiting 60 seconds...' % str(e))
                time.sleep(60)

        return


if __name__ == '__main__':
    pass
