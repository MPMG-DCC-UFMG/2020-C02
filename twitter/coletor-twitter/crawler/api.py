import copy
import datetime as date
import json
import os
import random
import requests
import tweepy
import time

from datetime import datetime
from dateutil import parser


def is_none_string(value):
    return str(value).strip().lower() in [ 'none', 'null' ]


def list_directory(folder):
    if not os.path.isdir(folder):
        return None

    return os.listdir(folder)


def is_username_folder(folder):
    inside_elements = list_directory(folder)

    return any([ 'perfil.json' in filename.lower() for filename in inside_elements ]) \
        and any([ 'followers' in filename.lower() for filename in inside_elements ])


def filename_to_json(filename):
    Json = None

    if os.path.isfile(filename):
        with open(filename, 'rt') as fr:
            try:
                Json = json.load(fr)
            except:
                pass

    return Json


def filename_to_profiles(filename):
    if not os.path.isfile(filename):
        return []

    answer = []
    with open(filename, 'rt') as fr:
        while True:
            l = fr.readline()
            if not l:
                break
            if not l.strip():
                continue
            try:
                answer.append(str(int(l.strip())))
            except:
                try:
                    answer.append(json.loads(l.strip()))
                except:
                    continue

    return answer


def username_folder_to_json(folder, outside_folder, inside_folder):
    Json = {
        'perfil': filename_to_json(folder + 'perfil.json'),
        'tweets': sorted([ filename_to_json(folder + 'posts/' + filename) \
            for filename in list_directory(folder + 'posts/') ], \
            key=lambda x: str(x['created_at'])[:16])[::-1],
        'followers': filename_to_profiles(folder + 'followers.txt'),
        'following': filename_to_profiles(folder + 'following.txt')
    }

    if contains_media(folder):
        assign = copy.deepcopy(outside_folder)
        if not assign.endswith('/'):
            assign += '/'
        assign += folder.replace(inside_folder, '') + 'medias/'

        Json['media_folder'] = assign

    return Json


def contains_media(folder):
    return 'medias' in list_directory(folder) and list_directory(folder + '/medias/')


def hashtag_folder_to_json(folder, outside_folder, inside_folder):
    Json = {
        'tweets': sorted([ filename_to_json(folder + 'posts/' + filename) \
            for filename in list_directory(folder + 'posts/') ], \
            key=lambda x: str(x['created_at'])[:16])[::-1],
    }

    if contains_media(folder):
        assign = copy.deepcopy(outside_folder)
        if not assign.endswith('/'):
            assign += '/'
        assign += folder.replace(inside_folder, '') + 'medias/'

        Json['media_folder'] = assign

    return Json


def randomized(mlist):
    """
    Retorna uma versão aleatória da lista passada como argumento.
    Também funciona com sets e frozensets.
    """
    if type(mlist) != list:
        try:
            mlist = list(mlist)
        except:
            pass

    list_answer = copy.deepcopy(mlist)
    random.shuffle(list_answer)

    return list_answer


def make_unique(mlist):
    answer = []
    for element in mlist:
        if element not in answer:
            answer.append(element)
    
    return answer


def date_str(dt=None):
    """
    Retorna uma string que representa a data e hora.
    Pode receber um objeto datetime ou retorna sobre o
    horário atual.

    Parametros
    ----------
    dt : objeto datetime
        Data e horário a trasnformar em string. Defaut é a data
        corrente.

    Retorno
    -------
    str
        Data e hora do parametro dado.
    """

    if dt is None:
        return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    return dt.strftime("[%Y-%m-%d %H:%M:%S]")


def dumps(sts):
    """
    Retorna a string em json dos atributos de interesse num tweet.
    As duas coletas usam essa função para filtragem.

    Parametros
    ----------
    sts : Status
        Um objeto do tweepy que representa um tweet.

    Retorna
    -------
    str
        Texto json do tweet
    """
    status = {}
    status['id'] = sts.id_str
    status['text'] = sts.full_text
    status['in_reply_to_status_id'] = sts.in_reply_to_status_id_str
    status['in_reply_to_user_id'] = sts.in_reply_to_user_id_str
    status['created_at'] = str(sts.created_at)
    status['quote_count'] = sts.quote_count \
        if hasattr (status, 'quote_count') else None
    status['reply_count'] = sts.reply_count \
        if hasattr (status, 'reply_count') else None
    status['retweet_count'] = sts.retweet_count
    status['favorite_count'] = sts.favorite_count
    status['location'] = sts.location \
        if hasattr (status, 'location') else None

    # Retweet info
    try:
        status['retweet_id'] = sts.retweeted_status.id_str
        status['retweeted_user_id'] = sts.retweeted_status.user.id_str
    except Exception:
        status['retweet_id'] = None
        status['retweeted_user_id'] = None

    
    # Media info
    # if str(status['id']) == '':
        # print(sts.extended_entities['media'])
    try:
        medias = list()
        for image in sts.extended_entities['media']:
            if image['type'] == 'photo':
                url = image['media_url_https']

            elif image['type'] == 'video':
                bit = lambda x: x['bitrate'] if 'bitrate' in x else 0
                url = max(image['video_info']['variants'], key=bit)['url']
            
            medias.append((image['type'], url))
        status['medias'] = medias
    except Exception:
        status['medias'] = []

    # if str(status['id']) == '':
        # print('>>>>>> medias ==' + str(status['medias']))
        # exit(0)

    return json.dumps(status, ensure_ascii=False)


def dumps_perfil(usr):
    """
    Retorna a string em json dos atributos de interesse num perfil.

    Parametros
    ----------
    sts : User
        Um objeto do tweepy que representa um usuário.

    Retorna
    -------
    str
        Texto json do usuário.
    """
    perfil = {}
    perfil['id'] = usr.id
    perfil['screen_name'] = usr.screen_name
    perfil['name'] = usr.name
    perfil['location'] = usr.location
    perfil['description'] = usr.description
    perfil['protected'] = usr.protected
    perfil['followers_count'] = usr.followers_count
    perfil['friends_count'] = usr.friends_count
    perfil['created_at'] = str(usr.created_at)
    perfil['favourites_count'] = usr.favourites_count
    return json.dumps(perfil, ensure_ascii=False)


class shell:
    """
    Encapsula todas as funções e interações da coleta de tweets.


    Atributos
    ----------
    words : list de str
        Lista de palavras-chave 
    users : list de str
        Lista de usuários a serem monitoradas
    type : str
        Se a coleta é de 'users' ou 'keywords'
    ### NO FUTURO SERÁ VIA KAFKA
    output : str
        Pasta em que escrevemos os dados da coleta
    min : objeto datetime
        Data limite inferior da coleta.
    max : objeto datetime
        Data limite superior da coleta.
    timestamp : str
        Horáro UNIX em que a coleta começou. Importante pois
        é o nome da pasta em que fica salva a coleta.

    Métodos
    -------
    start(verbose)
        Inicializa a coleta de tweets, seja por meio de
        palavras-chave, seja por meio de usuários.
    """

    def __init__(self, Json, kind):
        """
        A inicialização faz as seguintes tarefas:
            1. Faz o login em todos os tokens de acesso
            2. Inicializa os tempos dos tokens
            3. Verifica o tipo de coleta e carrega os users/words
            4. Carrega as demais informações

        Parametros
        ----------
        Json : str
            Informações de entrada para configurar o objeto em
            formato json em uma string:
            1. lista de tokens autenticáveis
            2. tipo da coleta
            3. palavras/usuários da coleta
            4. saídas da coleta #### NO FUTURO DIRETO PRO KAFKA
        """
        Json = json.loads(Json)
        self.type = copy.deepcopy(kind)
        
        if Json['crawler'].lower().strip() != 'twitter':
            raise Exception("Not prepared to crawl from \"%s\"" % Json['crawler'].strip())

        self.__api = list()
        for t in Json['tokens']:
            auth = tweepy.OAuthHandler(
                t['consumer key'], t['consumer secret'])
            auth.set_access_token(
                t['access token'], t['access token secret'])

            api_object = tweepy.API(auth)
            try:
                _ = api_object.me()
                self.__api.append(api_object)
            except:
                pass
        self.__api = randomized(self.__api)
        # except: # raise Exception("Erro de autorização.")
        
        if len(self.__api) == 0:
            raise Exception("Erro de autorização.")
        
        self.curr = 0
        self.followers_limit = None
        self.following_limit = None
        self.just_ids = True
        self.outside_data_folder = Json['output']
        self.times = [date.datetime.now() - date.timedelta(minutes=15)
                for i in range(len(self.__api))]

        try:
            # self.type = Json['type']
            if 'get_follow_profiles' in Json:
                if str(Json['get_follow_profiles']).lower().strip() in [ 'false', 'true' ]:
                    Json['get_follow_profiles'] = str(Json['get_follow_profiles']).lower().strip().replace('false', '0').replace('true', '1')
                self.just_ids = not int(Json['get_follow_profiles'])

            if 'followers_limit' in Json:
                self.followers_limit = int(Json['followers_limit']) \
                    if not is_none_string(Json['followers_limit']) else None
                self.following_limit = copy.deepcopy(self.followers_limit)

            if self.type == 'keywords':
                if 'words' in Json:
                    Json['keywords'] = copy.deepcopy(Json['words'])

                self.words = make_unique([ word for word in Json['keywords'] if word.strip() ]) \
                    if 'keywords' in Json else []
            elif self.type == 'users':
                users = make_unique(Json['users']) if 'users' in Json else []
                self.users ={self.__api[0].get_user(screen_name=x).id_str:
                    x for x in users}
            else:
                raise Exception("Erro de tipo de coleta.")

        except tweepy.error.TweepError as e:
            if 'user not found' in str(e).lower():
                raise Exception("User not found: @%s" % [ username for username in users if not self.user_exists(username) ][0])
            else:
                raise Exception("Erro da API.")
        except Exception as e:
            raise Exception("Erro de entrada: %s." % str(e))

        ### NO FUTURO A SAÍDA SERÁ VIA KAFKA
        try:
            self.output = '/var/twitter-crawler/'
            # self.output = Json['output']
            if self.output[-1] != '/':
                self.output += '/'

            self.min = parser.parse(Json['min_date']) if 'min_date' in Json and not is_none_string(Json['min_date']) else parser.parse('2001-01-01')
            self.max = parser.parse(Json['max_date']) if 'max_date' in Json and not is_none_string(Json['max_date']) else parser.parse(str(date.datetime.now() + date.timedelta(days=4)).split()[0])
        except Exception as e:
            raise Exception("Erro de entrada: %s" % str(e))

        # keep input json on class
        self.__json = copy.deepcopy(Json)
        
        # setting users ans tags available to download
        self.downloading_users = self.get_downloading_available('users')
        self.downloading_words = self.get_downloading_available('words')


    def get_downloading_available(self, pattern):
        """
        Guarda quais usuários/palavras devem ser considerados na coleta.
        Verfica se estão descritos no json de entrada.
        """

        download_key_pattern = pattern + '_to_download_media'

        if download_key_pattern in self.__json and self.__json[download_key_pattern] is None:
            del self.__json[download_key_pattern]

        assign_set = frozenset()

        if pattern in self.__json:
            if download_key_pattern in self.__json:
                assign_set = frozenset([ entity for entity in self.__json[download_key_pattern] if user in self.__json[pattern] ])
            else:
                assign_set = frozenset(self.__json[pattern])

        return assign_set


    def user_exists(self, username):
        exists = False
        try:
            _ = self.__api[0].get_user(screen_name=username).id_str
            exists = True
        except:
            pass

        return exists


    def joined_timestamps(self, mlist):
        """
        Recebe uma lista de timestamps e o busca na pasta "data" e
        retorna a junção de usuários e hashtags contidos em
        quaisquer desses timestamps
        """

        Json = {}
        for timestamp in mlist:
            name = self.output + str(timestamp) + '/'
            if os.path.isdir(name):
                dir_names = list_directory(name)
                for usertag in dir_names:
                    if usertag not in Json:
                        Json[usertag] = {}
                    if is_username_folder(name + usertag + '/'):
                        Json[usertag]['user'] = username_folder_to_json(name + usertag + '/', self.outside_data_folder, self.output)
                    else:
                        Json[usertag]['keyword'] = hashtag_folder_to_json(name + usertag + '/', self.outside_data_folder, self.output)
                        
        return Json


    def start(self, verbose=False):
        """
        Inicializa a coleta de tweets, seja por meio de
        palavras-chave, seja por meio de usuários.

        Parametros
        ----------
        verbose : bool
            Se a coleta deve imprimir toda ação realizada (e.g,
            usuário publicou, o coletor entrou em repouso etc).
        """
        if verbose:
            print(date_str() + " Início da coleta")

        self.__folders()

        if self.type == 'keywords':
            self.__keywords(verbose)
        elif self.type == 'users':
            self.__profile_data()
            self.__follow('followers', verbose)
            self.__follow('following', verbose)
            self.__users(verbose) 

        return self.timestamp


    def __folders(self):
        """
        Cria as pastas de organização da coleta
        """
        ### POR ENQUANTO ESCREVE EM ARQUIVO. NO FUTURO, NÃO
        ### PRECISAREMOS DE PASTAS COMO ESTAS
        self.timestamp = str(int(time.time()*1000))
        folder = self.output + self.timestamp + '/'
        if not os.path.exists(folder):
            os.makedirs(folder)

        if self.type == 'keywords':
            names = self.words
        elif self.type == 'users':
            names = {v for _, v in self.users.items()}

        for  v in names:
            new = folder + v
            if not os.path.exists(new):
                os.makedirs(new)
            if not os.path.exists(new+"/posts"):
                os.makedirs(new+"/posts")
            if not os.path.exists(new+"/medias"):
                os.makedirs(new+"/medias")


    def __request_media(self, filename, url):
        """
        Faz a requisição da mídia e escreve no disco.

        Parametros
        ----------
        filename : str
            Nome da mídia no disco.
        url : str
            Endereço url da mídia.
        """
        r = requests.get(url) 
        time.sleep(random.uniform(0, 4))
        with open(filename, 'wb') as f: 
            f.write(r.content) 


    def __iter_media(self, post_path):
        """
        Baixa as mídias de um dado post no twitter, se for de um
        usuário em users.

        Parametros
        ----------
        post : dict
            Um post de twitter que se deseja baixar as mídias (se
            existirem)
        """
        try:
            post = post_path[0]
            path = post_path[1] + post['id']
            num = len(post['medias']) > 1
            key = 1 
            for photo in post['medias']:
                count = '_' + str(key) if num else ''
                if photo[0] == 'photo':
                    filename = path + count + '.jpg' 
                elif photo[0] == 'video':
                    filename = path + count + '.mp4' 
        
                self.__request_media(filename, photo[1])
                key += 1
            return True

        except:
            return False

    
    def __append(self, msg, name, id_post, download_media=False):
        """
        Salva texto do post na pasta adequada. 
        """
        ### POR ENQUANTO ESCREVE EM ARQUIVO. NO FUTURO, ESSA FUNÇÃO
        ### MANDARIA A MENSAGEM PARA O KAFKA
        media_name = self.output + self.timestamp + '/' + name + '/medias/'
        name = self.output + self.timestamp + '/' + name + \
                '/posts/' + str(id_post) + '.json'

        with open(name, 'w') as f:
            f.write(msg)

        if download_media and json.loads(msg)['medias']:
            _ = self.__iter_media(( json.loads(msg), media_name ))


    def __next_api(self):
        """
        Função que muda o apontador para a api subsequente
        """
        self.curr = (self.curr + 1) % (len(self.__api))

    
    def __rate_limit(self, verbose):
        """
        Função que é chamada na exceção de rate limit. Troca os
        tokens e espera caso seja necessário.
        """
        if verbose:
            print(date_str() + " Coletor dormiu...")
        
        self.times[self.curr] = date.datetime.now()
        self.__next_api()

        time.sleep(60)

        # self.curr = (self.curr + 1) % (len(self.__api))
        # diff = date.datetime.now() - self.times[self.curr]
        # diff = int(diff.total_seconds())
        
        # if verbose:
            # print(date_str() + " Troca de tokens.")
        
        # if(diff < 900):
            # time.sleep(900 - diff)
        
        if verbose:
            print(date_str() + " Coletor acordou!")


    def __keywords(self, verbose):
        """
        Coleta posts com as palavras-chave definidas no período.

        Parametros
        ----------
        verbose : bool
            Se a coleta deve imprimir toda ação realizada (e.g,
            usuário publicou, o coletor entrou em repouso etc).
        """
        for u in self.words:
            q_search = u # if u.startswith('#') else '#' + u
            is_download_available = u in self.downloading_words

            cursor = tweepy.Cursor(
                self.__api[self.curr].search,
                q=q_search,
                include_entities=True,
                since=self.min,
                until=self.max,
                tweet_mode="extended")
            c = cursor.items()
            max_id = 1e+20

            while True:
                try:
                    status = c.next() 

                    if verbose:
                        last = status.created_at - date.timedelta(hours=3)
                        dt = date_str(last)
                        print(dt + " tweet de {}".format(u))

                    max_id = status.id-1
                    text = dumps(status)
                    self.__append(text, u, status.id, download_media=is_download_available)
                except tweepy.RateLimitError:
                    self.__rate_limit(verbose)
            
                    cursor = tweepy.Cursor(
                        self.__api[self.curr].search,
                        q=u,
                        include_entities=True,
                        since=self.min,
                        until=self.max,
                        max_id=max_id,
                        tweet_mode="extended")
                    c = cursor.items()

                    continue

                except StopIteration:
                    break

                except Exception as e:
                    print(str(e))
                    print("Erro na coleta de ", u)
                    break 


    def __profile_data(self):
        """
        placeholder
        """
        for u in self.users:
            data = self.__api[self.curr].get_user(id=u)
            msg = dumps_perfil(data)
        
            name = self.output + self.timestamp + '/' + \
                self.users[u] + '/perfil.json'

            with open(name, 'w') as f:
                f.write(msg)


    def __individual_follow(self, user, screen_name, kind, just_ids, is_root_user, verbose):
        msg =  " Novo bloco de @{}. Espera de 60 segundos.".format(
            screen_name) # cada bloco possui 5000 seguidores
        msg_limit_reached =  " Número de {} de @{} superou o limite permitido ({}).".format(
            kind, screen_name, str(self.followers_limit))
        msg_total_crawled =  " Parcial de {} de @{}: ".format(
            kind, screen_name)
        name_cursor = self.output + self.timestamp + '/' + \
            screen_name + '/pointer_' + kind + 's' + '.txt'

        profiles = []
        total_crawled = 0
        next_cursor = (None, -1)
        limit_crawling = self.followers_limit if kind == 'followers' else self.following_limit
        while next_cursor[1] is not None:
        # for p in pages:
            try:
                if just_ids:
                    if kind == 'followers':
                        api_answer = self.__api[self.curr].followers_ids(user, cursor=next_cursor[1]) if next_cursor[1] else None
                    else:
                        api_answer = self.__api[self.curr].friends_ids(user, cursor=next_cursor[1]) if next_cursor[1] else None
                else:
                    if kind == 'followers':
                        api_answer = self.__api[self.curr].followers(user, cursor=next_cursor[1]) if next_cursor[1] else None
                    else:
                        api_answer = self.__api[self.curr].friends(user, cursor=next_cursor[1]) if next_cursor[1] else None

                self.__next_api()
            except tweepy.RateLimitError as e:
                print('error: %s' % str(e))
                time.sleep(60)
                self.__rate_limit(verbose)
                continue
            except tweepy.TweepError as e:
                if 'not authorized' in str(e).lower():
                    profiles = None
                    break
                print('error: %s' % str(e))
                time.sleep(60)
                continue
            if not api_answer:
                api_answer = [ [], (None, None) ]
            
            if verbose:
                print(date_str() + msg) 
            # time.sleep(60)

            p, next_cursor = api_answer
            dump_cursor = copy.deepcopy(next_cursor[1])

            # ids.extend(p)
        
            for usr in p:
                if total_crawled < limit_crawling:
                    profiles.append(usr if not just_ids else str(usr))
                total_crawled += 1

            if is_root_user:
                with open(name_cursor, 'w') as f:
                    f.write(str(dump_cursor) + '\n')

            if verbose:
                print(msg_total_crawled + str(total_crawled))

            if limit_crawling is not None and total_crawled >= limit_crawling:
                if verbose:
                    print(msg_limit_reached)
                break


        return profiles


    def __follow(self, kind, verbose):
        """
        placeholder
        """

        if verbose:
            print(date_str() + " Coleta de %s." % kind)

        for u in self.users:
            name = self.output + self.timestamp + '/' + \
                self.users[u] + '/' + kind + '.txt'

            # ids = list()
            # pages = tweepy.Cursor(self.__api[
            # self.curr].followers_ids, id=u).pages()
              
            # testando retorno para cursor = 0
            # print(self.__api[self.curr].followers_ids(u, cursor=0))

            follow_profiles = self.__individual_follow(u, self.users[u], kind, just_ids=self.just_ids, is_root_user=True, verbose=verbose)
            if follow_profiles is not None and not self.just_ids:
                as_dictlist = []
                for inside_profile in follow_profiles:
                    dump_version = dumps_perfil(inside_profile)
                    jason = json.loads(dump_version)

                    jason['followers_ids'] = self.__individual_follow(jason['id'], jason['screen_name'], 'followers', just_ids=True, is_root_user=False, verbose=verbose)
                    jason['following_ids'] = self.__individual_follow(jason['id'], jason['screen_name'], 'following', just_ids=True, is_root_user=False, verbose=verbose)
                    
                    as_dictlist.append(json.dumps(jason))
                follow_profiles = as_dictlist

            if follow_profiles is None:
                with open(name, 'wt+') as fw:
                    fw.write('not authorized to get %s of profile id %s (%s)\n' % (kind, u, self.users[u]))
            else:
                with open(name, 'wt+') as fw:
                    for inside_profile in follow_profiles:
                        fw.write(inside_profile + '\n')

        return
            

    def __users(self, verbose):
        """
        Coleta posts dos perfis selcionados no período definido e
        salva em arquivos próprios dentro do diretório definido em output.

        Parametros
        ----------
        verbose : bool
            Se a coleta deve imprimir toda ação realizada (e.g,
            usuário publicou, o coletor entrou em repouso etc).
        """
        for u in self.users:
            is_download_available = self.users[u] in self.downloading_users
            cursor = tweepy.Cursor(
                self.__api[self.curr].user_timeline,
                id=u,
                include_entities=True,
                tweet_mode="extended")
            c = cursor.items()
            max_id = 1e+20

            while True:
                try:
                    status = c.next() 
                    if status.created_at - \
                            date.timedelta(hours=3) > self.max:
                        continue

                    if status.created_at - \
                            date.timedelta(hours=3) < self.min:
                        raise StopIteration

                    if verbose:
                        last = status.created_at - date.timedelta(hours=3)
                        dt = date_str(last)
                        print(dt + " tweet de @{}".format(self.users[u]))

                    max_id = status.id-1
                    text = dumps(status)
                    self.__append(text, self.users[u], status.id, download_media=is_download_available)

                except tweepy.RateLimitError:
                    self.__rate_limit(verbose)

                    cursor = tweepy.Cursor(
                        self.__api[self.curr].user_timeline,
                        id=u,
                        include_entities=True,
                        max_id = max_id,
                        tweet_mode="extended")
                    c = cursor.items()

                    continue

                except StopIteration:
                    break

                except Exception as e:
                    print("Erro na coleta de %s: %s" % (self.users[u], str(e)))
                    break 
