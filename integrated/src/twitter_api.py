import common_functions as common
import copy
import datetime as date
import json
import os
import random
import requests
import time

#Bibliotecas referentes à transição para versão 2v da API.

from TwitterAPI import *
from TwitterAPI.TwitterAPI import HydrateType



from datetime import datetime
from dateutil import parser


TOPIC_KAFKA_TWITTER_FOLL = 'crawler_twitter_seg'
TOPIC_KAFKA_TWITTER_PROFILE = 'crawler_twitter_perfil'
TOPIC_KAFKA_TWITTER_POST = 'crawler_twitter_post'

def translate_json_keys(jason, source, target, recursively=False):
    """
    Função que recebe um objeto json
    e traduz suas chaves para portugues
    """

    translated = {}
    if recursively:
        translated = translate_node(jason, source, target)
    elif type(jason) == dict:
        for k in jason:
            translated[translate_word(k, source, target)] = jason[k]
    else:
        translated = jason

    return translated


def translate_word(word, source, target):
    translation = {
        'pt': { 'en': {} },
        'en': {
            'pt': {
            'access token': 'token_acesso',
                'access token secret': 'segredo_token_acesso',
                'author' : 'usuario_do_autor',
                'author_id': 'id_do_autor',
                'consumer key': 'chave_consumidor',
                'consumer secret': 'segredo_consumidor',
                'crawler': 'coletor',
                'created_at': 'criado_em',
                'description': 'descricao',
                'error': 'erro',
                'favorite_count': 'quant_curtidas',
                'favourites_count': 'quant_curtidas',
                'following': 'seguindo',
                'followers': 'seguidores',
                'followers_count': 'quant_seguidores',
                'followers_limit': 'limite_de_seguidores',
                'friends': 'seguindo',
                'friends_count': 'quant_seguindo',
                'get_follow_profiles': 'recuperar_perfis_de_seguidores',
                'id': 'identificador',
                'interaction_id': 'id_do_tweet_de_interacaoo',
                'in_reply_to_status_id': 'em_resposta_ao_tweet_id',
                'in_reply_to_user_id': 'em_resposta_ao_usuario_id',
                'interactor_user_id': 'id_do_usuario_da_interacao',
                'keyword': 'palavra-chave',
                'location': 'localizacao',
                'location_id': 'id_localização',
                'max_date': 'data_max',
                'media_folder': 'pasta_midias',
                'medias': 'midias',
                'min_date': 'data_min',
                'name': 'nome',
                'output': 'pasta_da_saida',
                'profile': 'perfil',
                'profile_image_url': 'url_imagem_perfil_usuario',
                'protected': 'protegido',
                'quote_count': 'quant_citacoes',
                'replied_to': 'id_do_tweet_respondido',
                'reply_count': 'quant_respostas',
                'retweet_count': 'quant_retuites',
                'retweet_id': 'id_do_tweet_retuitado',
                'retweeted_user_id': 'id_do_usuario_retuitado',
                'screen_name': 'nome_de_exibicao',
                'text': 'texto',
                'tokens': 'chaves_de_acesso',
                'tweets': 'tweets',
                'tweet_count': 'qtde_de_tweets_realizados',
                'type': 'tipo_de_interacao',
                'user': 'usuario',
                'url': 'url',
                'users': 'usuarios',
                'user_created_at' : 'data_de_criacao_do_usuario',
                'user_location': 'localizacao_do_usuario',
                'users_to_download_media': 'usuarios_a_baixar_midias',
                'verified': 'verificado',
                'words': 'palavras',
                'words_to_download_media': 'palavras_a_baixar_midias',
}
        }
    }
    for w in translation['en']['pt']:
        translation['pt']['en'][translation['en']['pt'][w]] = w

    if word not in translation[source][target]:
        print('cannot find translation of "%s" from %s to %s' % (word, source, target))
        exit(0)

    return translation[source][target][word]


def could_be_list(mlist):
    is_ok = False
    try:
        _ = list(mlist)
        is_ok = True
    except:
        pass

    return is_ok


def translate_node(jason, source, target):
    if type(jason) == dict:
        new_object = {}
        for k in jason:
            new_object[translate_word(k, source, target)] = translate_node(jason[k], source, target)
        return new_object

    if could_be_list(jason):
        frozen_later = type(jason) == frozenset
        new_type = type(jason) if not frozen_later else set()
        new_object = new_type()

        if 'append' in dir(new_object):
            for element in jason:
                new_object.append(translate_node(element, source, target))
            return new_object

        if 'add' in dir(new_object):
            for element in jason:
                new_object.add(translate_node(element, source, target))
            return new_object if not frozen_later else frozenset(new_object)

        return jason

    return jason


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
        'profile': filename_to_json(folder + 'perfil.json'),
        'tweets': sorted([ filename_to_json(folder + 'posts/' + filename) \
            for filename in list_directory(folder + 'posts/') ], \
            key=lambda x: str(x['criado_em'])[:16])[::-1],
        'followers': filename_to_profiles(folder + 'followers.json'),
        'following': filename_to_profiles(folder + 'following.json')
    }

    if contains_media(folder):
        assign = copy.deepcopy(outside_folder)
        if not assign.endswith('/'):
            assign += '/'
        assign += folder.replace(inside_folder, '') + 'medias/'

        Json['media_folder'] = assign

    return translate_json_keys(Json, 'en', 'pt')
    # return Json


def contains_media(folder):
    return 'medias' in list_directory(folder) and list_directory(folder + '/medias/')


def hashtag_folder_to_json(folder, outside_folder, inside_folder):
    Json = {
        'tweets': sorted([ filename_to_json(folder + 'posts/' + filename) \
            for filename in list_directory(folder + 'posts/') ], \
            key=lambda x: str(x['criado_em'])[:16])[::-1],
    }

    if contains_media(folder):
        assign = copy.deepcopy(outside_folder)
        if not assign.endswith('/'):
            assign += '/'
        assign += folder.replace(inside_folder, '') + 'medias/'

        Json['media_folder'] = assign

    return translate_json_keys(Json, 'en', 'pt')
    # return Json


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
        Data e horário a transformar em string. Defaut é a data
        corrente.

    Retorno
    -------
    str
        Data e hora do parametro dado.
    """

    if dt is None:
        return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    return dt.strftime("[%Y-%m-%d %H:%M:%S]")

def is_valid_date(value,var_type, comp_type):
    """
    Checa se a data passada está em um formato válido, e se está num intervalo que garanta o funcionamento da coleta por keywords
    e de usuários.

    Parametros
    ----------
    value : str 
            Contem a data passada, no formato YYYY-MM-DD,
            utilizada na comparação.

    var_type : str
            Contem o tipo de verificação a ser realizada.
            Na execução do programa, pode assumir dois valores: 'keyword' ou 'users'
    

    Retorno
    -------
    bool
        True or False
    """
    #Data mínima aceitável para realizar a coleta de posts de usuários. Retirada da documentação do twitter.
    MINIMUM_ALLOWABLE_TIME = '2010-11-06'


    try:
        dt_object = datetime.strptime(value, "%Y-%m-%d")
        time_difference = datetime.now() - dt_object

        if var_type == "keywords":
            return True \
                if 0 < time_difference.days < 7 else False
        
        elif var_type == "users":
            return True \
                if 0 < time_difference.days and value > MINIMUM_ALLOWABLE_TIME else False

    except Exception:
        raise Exception(f'Data informada no campo {comp_type} é inválida para {var_type}.')

def get_user_parameters(status, parameters):
    
    status['author_id'] = parameters['id']
    
    status['screen_name'] = parameters['username']
    
    status['name'] = parameters['name']
    
    status['user_created_at'] = str(parameters['created_at'])
    
    status['user_location'] = parameters['location'] \
        if 'location' in parameters else ""
    
    status['description'] = parameters['description']
        
    status['verified'] = parameters['verified']
    
    status['profile_image_url'] = parameters['profile_image_url']  \
    if 'profile_image_url' in parameters else ""
    
    status['url'] = parameters['url'] \
    if 'url' in parameters else ""
    
    return status

def dumps(sts):
    """
    Retorna a string em json dos atributos de interesse num tweet.
    As duas coletas usam essa função para filtragem.

    Parametros
    ----------
    sts : Status
        Um objeto de tweet.

    Retorna
    -------
    str
        Texto json do tweet.
    """
    #Cria todos os atributos.
    status = {}
    status['id'] = sts['id']
    status['author_id'] = sts['author_id']
    status['text'] = sts['text']
    status['created_at'] = str(sts['created_at'])
    status['quote_count'] = sts['public_metrics']['quote_count']
    status['reply_count'] = sts['public_metrics']['reply_count']
    status['retweet_count'] = sts['public_metrics']['retweet_count']
    status['favorite_count'] = sts['public_metrics']['like_count']
    status['location'] = sts['geo']['place_id_hydrate']['full_name'] \
        if 'geo' in sts and 'full_name' in sts['geo']['place_id_hydrate'] else None
    # status['type'] = None
    # status['interactor_user_id'] = None
    # status['interaction_id'] = None
    status['retweet_id'] = None
    status['retweeted_user_id'] = None
    status['in_reply_to_status_id'] = None
    status['in_reply_to_user_id'] = None

    status = get_user_parameters(status, sts['author_id_hydrate'])
 
    
    status['medias'] = []


    #Verifica se existe algum tweet que interage com o tweet capturado. Se houver, pega o tipo o ID do tweet, e o ID do autor.
    if 'referenced_tweets' in sts:
        tweet_interaction = sts['referenced_tweets'][0]['type']
        id_tweet_interacao = sts['referenced_tweets'][0]['id']

        #Considerando que em alguns poucos casos a API do twitter não retorna o author_id.
        try:
            referenced_author_id = sts['referenced_tweets'][0]['id_hydrate']['author_id']
        except:
            pass

        if tweet_interaction == 'retweeted':
            
            try:
                status['retweeted_user_id'] = referenced_author_id 
            except:
                pass

            status['retweet_id'] = id_tweet_interacao
                    
        # elif tweet_interaction == 'quoted':
        #     status['type'] = "Citação"

        elif tweet_interaction == 'replied_to':
            status['in_reply_to_status_id'] = id_tweet_interacao
            status['in_reply_to_user_id'] = sts['in_reply_to_user_id'] \
                if 'in_reply_to_user_id' in sts else None   
    
    try:
        medias = list()
        for image in sts['attachments']['media_keys_hydrate']:
            if image['type'] == 'photo' or image['type'] == 'gif':
                url = image['url']
                medias.append((image['type'], url))
        status['medias'] = medias
    except:
        pass

    return json.dumps(translate_json_keys(status, 'en', 'pt'), ensure_ascii=False)

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
    perfil['id'] = usr['id']
    perfil['screen_name'] = usr['username']
    perfil['name'] = usr['name']
    perfil['created_at'] = str(usr['created_at'])
    perfil['location'] = usr['location'] \
        if 'location' in usr else ""
    perfil['description'] = usr['description']
    perfil['protected'] = usr['protected']
    perfil['followers_count'] = usr['public_metrics']['followers_count']
    perfil['friends_count'] = usr['public_metrics']['following_count']
    perfil['favourites_count'] = None
    #Listed_count != favourites_count
    #perfil['favourites_count'] = usr['public_metrics']['listed_count']
    #perfil['tweet_count'] = usr['public_metrics']['tweet_count']
    return json.dumps(translate_json_keys(perfil, 'en', 'pt'), ensure_ascii=False)

def credentials_to_api_object(token):
    try:
        api_object = TwitterAPI(
            token['consumer key'], 
            token['consumer secret'],
            token['access token'], 
            token['access token secret'],
            api_version='2',
        )
    except:
        api_object = None
    
    return api_object


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
    
    Novos parametros utilizados na requisição da coleta de tweets e perfis:

    expansions: str
        Correspondente ao campo 'expansions'
    tweet_fields: str
        Correspondente ao campo 'tweet.fields'
    media_fields: str
        Correspondente ao campo 'media.fields'
    user_fields: str
        Correspondente ao campo 'user.fields'
    place_fields:: str
        Correspondente ao campo 'place.fields'
    t_max_result: int
        Correspondente ao campo 'max_results' de buscas por tweets.
    f_max_result: int
        Correspondente ao campo 'max_result', na captura dos id's e perfis de seguidores/seguintes

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
        
        # if Json['crawler'].lower().strip() != 'twitter':
        #     raise Exception("Not prepared to crawl from \"%s\"" % Json['crawler'].strip())

        self.__api = list()
        for t in Json['tokens']:
            api_object = credentials_to_api_object(t)
            
            if api_object is not None:
                self.__api.append(api_object)

        self.__api = randomized(self.__api)
        
        if len(self.__api) == 0:
            raise Exception("Erro de autorização.")
        
        #Declaração dos valores utilizados para realizar a coleta ao longo do programa.
        self.__expansions = 'attachments.media_keys,referenced_tweets.id.author_id,geo.place_id'
        self.__tweet_fields = 'attachments,author_id,created_at,entities,geo,id,in_reply_to_user_id,public_metrics,text,referenced_tweets'
        self.__media_fields = 'url,type'
        self.__user_fields = 'created_at,verified,id,url,profile_image_url,protected,location,description,public_metrics'

        self.__place_fields = 'full_name'
        
        self.__t_max_results = 100
        
        self.__f_max_results = 1000

        #Erro 429: Limit Request Exceeded.
        #Caso algum dos pontos de comunicação com a API receba um erro com esse código, chama a função __rate_limit.
        self.__error_code = 429
        self.__error_message = 'Rate limit exceeded\n'

        self.curr = 0
        self.followers_limit = None
        self.following_limit = None
        self.just_ids = True
        self.output = '/datalake/ufmg/twitter/'
        self.outside_data_folder = '/datalake/ufmg/twitter/'
        self.times = [date.datetime.now() - date.timedelta(minutes=15)
                for i in range(len(self.__api))]

        self.times[0] = date.datetime.now()

        try:
            if 'get_follow_profiles' in Json:
                if str(Json['get_follow_profiles']).lower().strip() in [ 'false', 'true' ]:
                    Json['get_follow_profiles'] = str(Json['get_follow_profiles']).lower().strip().replace('false', '0').replace('true', '1')
                self.just_ids = not int(Json['get_follow_profiles'])

            if 'followers_limit' in Json:
                self.followers_limit = int(Json['followers_limit']) \
                    if not is_none_string(Json['followers_limit']) else None
                self.following_limit = copy.deepcopy(self.followers_limit)

            if self.type == 'keywords' or self.type == 'words':
                if 'words' in Json:
                    Json['keywords'] = copy.deepcopy(Json['words'])

                self.words = make_unique([ word for word in Json['keywords'] if word.strip() ]) \
                    if 'keywords' in Json else []
                

                #Formatação da data no formato ISO é necessária para comunicação com a API 2v do twitter.
                self.min = parser.parse(Json['min_date']).isoformat('T')[:]+'Z' \
                    if 'min_date' in Json and not is_none_string(Json['min_date']) \
                    and is_valid_date(Json['min_date'], "keywords", 'min_date') else None
                
                self.max = parser.parse(Json['max_date']).isoformat('T')[:]+'Z' \
                    if 'max_date' in Json and not is_none_string(Json['max_date']) \
                    and is_valid_date(Json['max_date'], "keywords", 'max_date') else None
            
                self.type = 'keywords'

            elif self.type == 'users':
                
                users = make_unique(Json['users']) if 'users' in Json else []
                
                self.min = parser.parse(Json['min_date']).isoformat('T')[:]+'Z' \
                    if 'min_date' in Json and not is_none_string(Json['min_date']) \
                    and is_valid_date(Json['min_date'], "users", 'min_date') else None
                
                self.max = parser.parse(Json['max_date']).isoformat('T')[:]+'Z' \
                    if 'max_date' in Json and not is_none_string(Json['max_date']) \
                    and is_valid_date(Json['max_date'], "users", 'max_date') else None

                self.users = {}
            
                # Para cada usuário, testa se é possível realizar a captura, e cria o dicionário user:id.
                # Seguinte trecho deveria estar em um método.
                # Posto aqui para evitar grandes modificações na estrutura elaborada pelo autor original do código.
                ###########
                for usuario in users:
                    
                    try:
                        var = self.__api[self.curr].request(f'users/by/username/:{usuario}').json()

                        if 'id' in var['data']:
                            self.users[var['data']['id']] = usuario
                    
                    except KeyError as e:
                        print("Usuário não encontrado")
                        continue

                    except TwitterRequestError as e:
                        
                        if e.status_code == self.__error_code:
                            if e.msg != self.__error_message:
                                self.__treat_ratelimit_exception(e.status_code, e.msg)
                                
                            self.__rate_limit(verbose=False)

                            var = self.__api[self.curr].request(f'users/by/username/:{usuario}').json()
                            
                            if 'id' in var['data']:
                                self.users[var['data']['id']] = usuario
                        else:
                            raise Exception('Erro na validação de usuários')

                    except KeyError as e:
                        print("Usuário não encontrado")
                        continue
               
            else:
                raise Exception("Erro de tipo de coleta.")



        except Exception as e:
            raise Exception("Erro de entrada: %s." % str(e))

        try:
            # self.output = '/var/twitter-crawler/'
            if self.output[-1] != '/':
                self.output += '/'
        
        except Exception as e:
            raise Exception("Erro de entrada: %s" % str(e))

        # keep input json on class
        self.__json = copy.deepcopy(Json)

        # setting users ans tags available to download
        self.downloading_users = self.get_downloading_available('users')
        self.downloading_words = self.get_downloading_available('words')

    def __treat_ratelimit_exception(self, error_code, message_error):
        """
        Função responsável por tratar a exceção de chaves esgotadas mensalmente.
        
        """
        if error_code == self.__error_code and message_error != self.__error_message:
            print(f'Erro: {message_error}\nAlguma chave inserida esta esgotada. Favor conferir os limites')
            del self.__api[self.curr]
            
            del self.times[self.curr]

            
            if len(self.__api) == 0:
                raise Exception('Todas as chaves estao esgotadas.')    
            
            self.__next_api()
            return True
        else:
            raise Exception(f"Erro {error_code}: {message_error}")

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
                assign_set = frozenset([ entity for entity in self.__json[download_key_pattern] if entity in self.__json[pattern] ])
            else:
                assign_set = frozenset(self.__json[pattern])

        return assign_set

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
                        Json[usertag][translate_word('user', 'en', 'pt')] = username_folder_to_json(name + usertag + '/', self.outside_data_folder, self.output)
                    else:
                        Json[usertag][translate_word('keyword', 'en', 'pt')] = hashtag_folder_to_json(name + usertag + '/', self.outside_data_folder, self.output)
                        
        return Json


    def start(self, verbose=True):
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


        if self.type == 'keywords' and self.words:
            self.__folders()
            self.__keywords(verbose)

            return self.timestamp

        elif self.type == 'users' and self.users:
            self.__folders()
            self.__profile_data(verbose=True)
            
            if self.followers_limit == None or self.followers_limit > 0:

                self.__follow('followers', verbose)
                self.__follow('following', verbose)
                
            self.__users(verbose) 

            return self.timestamp

        return 0


    def __folders(self):
        """
        Cria as pastas de organização da coleta
        """
        ### POR ENQUANTO ESCREVE EM ARQUIVO. NO FUTURO, NÃO
        ### PRECISAREMOS DE PASTAS COMO ESTAS
        names = {}
        self.timestamp = str(int(time.time()*1000))
        folder = self.output + self.timestamp + '/'
        if not os.path.exists(folder):
            os.makedirs(folder)

        if self.type == 'keywords' and self.words:
            names = self.words
        elif self.type == 'users' and self.users:
            names = {v for _, v in self.users.items()}

        if len(names) == 0:
            raise Exception('User or Keywords not found to create directories.')

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
        time.sleep(random.uniform(0, 8))
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
            id_key = 'id' if 'id' in post else 'identificador'
            medias_key = 'medias' if 'medias' in post else 'midias'
            path = post_path[1] + post[id_key]

            num = len(post[medias_key]) > 1
            key = 1
            new_medias = []
            for photo in post[medias_key]:
                count = '_' + str(key) if num else ''
                if photo[0] == 'photo':
                    filename = path + count + '.jpg' 
                elif photo[0] == 'video':
                    filename = path + count + '.mp4' 
                photo.append(filename)
                # photo.append(filename.replace(INSIDE_DEFAULT_PATH, OUTSIDE_DEFAULT_PATH))

                if len(photo) == 3:
                    new_medias.append(dict(zip([ 'tipo', 'url', 'caminho' ], photo)))

                self.__request_media(filename, photo[1])
                key += 1
            return new_medias

        except:
            return []
    
    def __append(self, kafka_prod, msg, name, id_post, download_media=False):
        global TOPIC_KAFKA_TWITTER_POST
        
        """
        Salva texto do post na pasta adequada. 
        """
        ### POR ENQUANTO ESCREVE EM ARQUIVO. NO FUTURO, ESSA FUNÇÃO
        ### MANDARIA A MENSAGEM PARA O KAFKA
        media_name = self.output + self.timestamp + '/' + name + '/medias/'
        

        name = self.output + self.timestamp + '/' + name + \
                '/posts/' + str(id_post) + '.json'
        
        loaded_object = json.loads(msg)
        medias_key = 'medias' if 'medias' in json.loads(msg) else 'midias'

        if download_media and json.loads(msg)[medias_key]:
            medias = self.__iter_media(( json.loads(msg), media_name ))
            loaded_object[medias_key] = medias
            msg = json.dumps(loaded_object)
        
        common.publish_kafka_message(kafka_prod, TOPIC_KAFKA_TWITTER_POST, self.crawling_id, msg)
        with open(name, 'w') as f:
            f.write(msg)


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
        
        #API reseta o número de requisições a cada 15 minutos - 900sec -, 15s foram adicionados por questões de segurança. 
        API_TIME_RESET = 915 #Segundos

        API_TIME_SLEEP = 15 #Segundos


        while True:
            try:
                time_window = date.datetime.now() - self.times[self.curr]
                if time_window.seconds >= API_TIME_RESET:
                    break
                else:
                    time.sleep(API_TIME_SLEEP)
                    self.__next_api()
            except:
                break
                
        
        self.times[self.curr] = date.datetime.now()
        
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
            kafka_prod = common.connect_kafka_producer()
            q_search = u 
            is_download_available = u in self.downloading_words


            cursor = TwitterPager(self.__api[self.curr], 'tweets/search/recent', 
            {
                'query': q_search,
                'tweet.fields': self.__tweet_fields,
                'expansions': self.__expansions,
                'media.fields': self.__media_fields,
                'user.fields': self.__user_fields,
                'place.fields': self.__place_fields,
                'max_results': self.__t_max_results,
                'start_time': self.min,
                'end_time': self.max

            }, hydrate_type=HydrateType.APPEND)
            
            c = cursor.get_iterator()
            max_id = -1
            max_id_str = None

            while True:
                try:
                    status = c.__next__() 

                    if verbose:
                        print(f"Tweet de {u}: criado em {status['created_at']} ")

                    max_id = int(status['id'])-1
                    text = dumps(status)
                    self.__append(kafka_prod, text, u, status['id'], download_media=is_download_available)
                
                except TwitterRequestError as e:
                    
                    if e.status_code == self.__error_code:
                        if e.msg != self.__error_message:
                            self.__treat_ratelimit_exception(e.status_code, e.msg)
                            
                        self.__rate_limit(verbose)    

                        if max_id != -1:
                            max_id_str = str(max_id)

                        cursor = TwitterPager(self.__api[self.curr], 'tweets/search/recent', 
                        {
                            'query': q_search,
                            'tweet.fields': self.__tweet_fields,
                            'expansions': self.__expansions,
                            'user.fields': self.__user_fields,
                            'media.fields': self.__media_fields,
                            'place.fields': self.__place_fields,
                            'max_results': self.__t_max_results,
                            'start_time': self.min,
                            'end_time': self.max,
                            'until_id': max_id_str

                        }, hydrate_type=HydrateType.APPEND)
                        
                        c = cursor.get_iterator()
                        continue

                    else:
                        raise Exception('Erro no processo de troca de chaves.')

                except StopIteration:
                    break

                except Exception as e:
                    raise Exception(f"Erro na coleta de {u}: {e}")


    def __profile_data(self, verbose=True):
        global TOPIC_KAFKA_TWITTER_PROFILE
        """
        Função responsável por fazer a recuperação dos perfis dos usuários.
        Caso o perfil seja privado, o retira da lista de usuários após coletar seu perfil.
        """

        private_users_list = list()

        for u in self.users:            
            while True:
                try:
                    data = self.__api[self.curr].request(f'users/:{u}',{
                        'user.fields': self.__user_fields,
                        }).json()

                    msg = dumps_perfil(data['data'])
                    if verbose:
                        print(f"Coletando perfil de {self.users[u]}")

                    # name = self.output + self.timestamp + '/' + \
                    #     self.users[u] + '/perfil.json'

                    # with open(name, 'w') as f:
                    #     f.write(msg)
                   
                    #Pega o ID do usuário protegido e dispara uma mensagem.
                    if data['data']['protected'] == True:
                        private_users_list.append(u)
                        print(f"Usuario {self.users[u]} é privado.")
                    
                    common.publish_kafka_message(common.connect_kafka_producer(), \
                    TOPIC_KAFKA_TWITTER_PROFILE, self.crawling_id, msg)

                    break
                        
                except TwitterRequestError as e:
                    if e.status_code == self.__error_code:
                        if e.msg != self.__error_message:
                            self.__treat_ratelimit_exception(e.status_code, e.msg)
                            
                        self.__rate_limit(verbose)
                        continue              
                    else:
                        raise Exception('Erro no processo de troca de chaves.')

                except Exception as e:
                    raise Exception(f"Erro: {e}")

        for items in private_users_list:
            del self.users[items]


    def __follow(self, kind, verbose):
        global TOPIC_KAFKA_TWITTER_FOLL
        """
        Coleta os ids dos followers e following de um dado usuário. Além disso, retorna também o perfil de seguidores,
        caso seja especificado na entrada.

        Parametros
        ----------
        kind: str
              Determina se a função ira coletar os seguidores de um usuário ou quem este usuário segue.
              Pode assumir dois valores: 'following' e 'followers'
        
        verbose: bool
                Se a coleta deve imprimir toda ação realizada (e.g,
                usuário publicou, o coletor entrou em repouso etc).      
        """
        
        if verbose:
            print(date_str() + " Coleta de %s." % kind)
        
        for u in self.users:
            
            #Variáveis auxiliares para realizar a coleta.
            NEXT_TOKEN = None
            break_all = False
            sentinel = 0

            following_list = list()            
            ###

            while True:
                try:
                    cursor = self.__api[self.curr].request(f'users/:{u}/{kind}',{
                    'user.fields': self.__user_fields,
                    'max_results': self.__f_max_results,
                    'pagination_token': NEXT_TOKEN
                    })
                    
                    
                    for status in cursor:
                        if self.followers_limit != None:
                            if sentinel >= self.followers_limit: 
                                break_all = True
                                break 
                            else: 
                                sentinel +=1 
                        
                        if kind == 'followers' and not self.just_ids:
                            msg = dumps_perfil(status)
                            
                            kafka_object = { self.users[u] + '/' + kind: msg }         
                            
                            common.publish_kafka_message (common.connect_kafka_producer(), \
                            TOPIC_KAFKA_TWITTER_FOLL, self.crawling_id, json.dumps(kafka_object, ensure_ascii=False).replace("\\\"", '"'))
                        
                        following_list.append(status['id'])

                    #O Meta é um parâmetro sempre retornado.
                    try:
                        meta_data = cursor.json()['meta']
                    except:
                        raise Exception(f"Erro na coleta de {kind} do usuário {self.users[u]}. Parâmetro 'meta' não retornado.")

                    #Apesar de ser rendundantes a presença simultânea dos dois últimos parâmetros, evita quaisquer erros que possam ocorrer.
                    if break_all == True or meta_data['result_count'] == 0 or 'next_token' not in meta_data:
                        break
                    
                    NEXT_TOKEN = meta_data['next_token']


                except TwitterRequestError as e:
                    if e.status_code == self.__error_code:
                        if e.msg != self.__error_message:
                            self.__treat_ratelimit_exception(e.status_code, e.msg)
                            
                        self.__rate_limit(verbose)
                        continue                               
                    else:
                        raise Exception(f'Erro: {e}')

                except Exception as e:
                    raise Exception(f"Erro: {e}")

            # profile_folder = self.output + self.timestamp + '/' + \
            # self.users[u] + '/' + kind + '.json'
            
            # with open(profile_folder, 'w') as f:
            #     json.dump(following_list, f, ensure_ascii=False)
    
            kafka_object = { self.users[u] + '/' + kind + '_id_list': following_list }
            common.publish_kafka_message(common.connect_kafka_producer(), TOPIC_KAFKA_TWITTER_FOLL, \
            self.crawling_id, json.dumps(kafka_object).replace("\\\"", '"'))

    def __make_single_user_list(self, username):
        uid = None
        for key in self.users:
            if self.users[key].lower() == username.lower():
                uid = copy.deepcopy(key)
                break

        self.users = { uid: username } if uid is not None else {}

        return


    def download_foll(self, username, crawling_id, kind):
        self.crawling_id = crawling_id
        self.__make_single_user_list(username)
        self.__folders()
        self.__follow(kind, verbose=True)
        return


    def download_followers(self, username, crawling_id):
        return self.download_foll(username, crawling_id, 'followers')


    def download_following(self, username, crawling_id):
        return self.download_foll(username, crawling_id, 'following')
    

    def download_profile(self, username, crawling_id):
        self.crawling_id = crawling_id
        self.__make_single_user_list(username)
        self.__folders()
        return self.__profile_data(verbose=True)


    def download_single_user(self, username, crawling_id):
        self.crawling_id = crawling_id
        self.__make_single_user_list(username)
        self.__folders()
        return self.__users(verbose=True)


    def download_single_word(self, word, crawling_id):
        self.crawling_id = crawling_id
        self.words = [ word ]
        self.__folders()
        return self.__keywords(verbose=True)



    def __users(self, verbose):
        """
        Coleta posts dos perfis selcionados no período definido e
        salva em arquivos próprios dentro do diretório definido em output.
                        fw.write(user_id + '\n')

        Parametros
        ----------
        verbose : bool
            Se a coleta deve imprimir toda ação realizada (e.g,
            usuário publicou, o coletor entrou em repouso etc).
        """
        for u in self.users:
            kafka_prod = common.connect_kafka_producer()
            is_download_available = self.users[u] in self.downloading_users
            cursor = TwitterPager(self.__api[self.curr], f'users/:{u}/tweets', 
            {
                'tweet.fields': self.__tweet_fields,
                'expansions': self.__expansions,
                'media.fields': self.__media_fields,
                'place.fields': self.__place_fields,
                'user.fields': self.__user_fields,
                'max_results': self.__t_max_results,
                'start_time': self.min,
                'end_time': self.max

            }, hydrate_type=HydrateType.APPEND)
            
            c = cursor.get_iterator()
            max_id = -1
            max_id_str = None

            while True:
                try:
                    status = c.__next__() 
                    
                    if verbose:
                        print(f"Tweet de {self.users[u]}: criado em {status['created_at']} ")

                    max_id = int(status['id'])-1
                    text = dumps(status)
                    self.__append(kafka_prod, text, self.users[u], status['id'], download_media=is_download_available)
                
                except TwitterRequestError as e:
                    
                    if e.status_code == self.__error_code:
                        if e.msg != self.__error_message:
                            self.__treat_ratelimit_exception(e.status_code, e.msg)
                            
                        self.__rate_limit(verbose)                            
                        if max_id != -1:
                            max_id_str = str(max_id)

                
                        cursor = TwitterPager(self.__api[self.curr], f'users/:{u}/tweets', 
                        {
                            'tweet.fields': self.__tweet_fields,
                            'expansions': self.__expansions,
                            'media.fields': self.__media_fields,
                            'place.fields': self.__place_fields,
                            'max_results': self.__t_max_results,
                            'user.fields': self.__user_fields,
                            'start_time': self.min,
                            'end_time': self.max,
                            'until_id': max_id_str

                        }, hydrate_type=HydrateType.APPEND)
                        
                        c = cursor.get_iterator()
                        continue
                    else:
                        raise Exception(f'Erro: {e}')
                    

                except StopIteration:
                    break

                except Exception as e:
                    raise Exception("Erro na coleta de %s: %s" % (self.users[u], str(e)))
