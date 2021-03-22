#!/usr/bin/python

import sys
import os
from datetime import datetime

class DataCollection:
    """
        Contem metodos para coletar posts, comments, perfil e midia de posts.
        Atributos
        ----------
        filename_output: str
            Nome do arquivo de saida
        dataHandle: DataHandle class
            Instancia da classe DataHandle
        instaloaderInstance: Instaloder.context
            Contexto da classe local da biblioteca Instaloader. Armazena informacoes dos proxies
        instaloaderClass: Instalodader class
            Instancia da classe local da biblioteca Instaloader. Utilizada para chamar metodos de coletas do Instaloader
        collection_type: str
            Tipo da coleta para salvar nos documentos e facilitar recuperacao futura.
        Metodos
        -------
        collectProfile(username: str)
            Coleta informacoes do perfil 'username'
        collectPosts(data_min: data, data_max: data, post_limit: int, username: str, hashtag: str)
            Coleta posts de um usuario ou hahstag no periodo entre data_min e data_max e de acordo com o limite post_limit
        downloadPostMedia(post_id: str, media_url: str)
            Coleta midia de um post (video ou imagem)
        collectComments(post_id: str, comments_by_post_limit: int, line_debug_number: int)
            Coleta comentarios de um post de acordo com o limite comments_by_post
        """
    def __init__(self, filename_output, dataHandle, instaloaderInstance, instaloaderClass,
                 document_type, filepath_medias=None):
        self.filename_output = filename_output
        self.dataHandle = dataHandle
        self.instaloaderInstance = instaloaderInstance
        self.instaloaderClass = instaloaderClass
        self.document_type = document_type
        self.filepath_medias = filepath_medias

    def __getErrorDocument(self, exception_obj, exc_type, exc_tb):
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        error_str = '{}'.format(str(exception_obj))
        error_details = '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno)

        error_document = {"erro": error_str, "detalhes": error_details, "data_e_hora": str(datetime.now())}

        return error_document

    def __getProfileDocument(self, profile_object):
        user_profile = {"identificador": str(profile_object.userid),
                        "nome_do_usuario": str(profile_object.username),
                        "nome_completo": str(profile_object.full_name).replace("\n",""),
                        "numero_de_seguidores": int(profile_object.followers),
                        "numero_de_seguidos": int(profile_object.followees),
                        "biografia": str(profile_object.biography).replace("\n",""),
                        "tipo_documento": self.document_type}

        return user_profile

    def collectProfile(self, username):
        error_document = None
        has_error = False
        try:
            profile = self.instaloaderClass.Profile.from_username(self.instaloaderInstance.context, username)

            user_profile_document = self.__getProfileDocument(profile_object=profile)

            self.dataHandle.persistData(filename_output=self.filename_output,
                                        document_list=[user_profile_document],
                                        operation_type="a")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error_document = self.__getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)
            has_error = True

        finally:
            return(has_error,error_document)


    def collectPosts(self, data_min, data_max, post_limit, username=None,hashtag=None):
        inserted_posts = 0
        error_document = None
        has_error = False

        try:
            if username is not None:
                instragram_source_object = self.instaloaderClass.Profile.from_username(self.instaloaderInstance.context, username)
                identificador_coleta = None

            else:
                instragram_source_object = self.instaloaderClass.Hashtag.from_name(self.instaloaderInstance.context,name=hashtag)
                identificador_coleta = hashtag

            posts = instragram_source_object.get_posts()

            for post in posts:
                try:
                    post_date = self.dataHandle.getDateFormatted(str(post.date))
                    if post_limit is not None and inserted_posts >= post_limit:
                        break

                    if hashtag is None and data_min is not None and  post_date < data_min:
                        break

                    if hashtag is None and data_max is not None and post_date > data_max:
                        pass
                    else:
                        ### Testa no caso de hashtag pois nao ha garantia de ordenamento
                        if hashtag is None or (hashtag is not None and post_date >= data_min and post_date <= data_max):
                            inserted_posts +=1
                            print("\tPosts coletados: {}\tData postagem {} ".format(inserted_posts, str(post.date)),
                                  '\tData e hora: ', datetime.now(), flush=True)

                            post_document = self.__getPostDocument(post_object=post,identificador_coleta=identificador_coleta)

                            self.dataHandle.persistData(filename_output=self.filename_output,
                                                        document_list=[post_document],
                                                        operation_type="a")



                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    error_document = self.__getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)
                    has_error = True
                    break

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error_document = self.__getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)
            has_error = True
        finally:
            return (has_error, error_document)

    def __getPostDocument(self, post_object, identificador_coleta=None):
        post_document = {"identificador": str(post_object.shortcode),
                        "identificador_usuario":str(post_object.owner_id),
                        "texto": post_object.caption,
                        "numero_likes": int(post_object.likes),
                        "numero_comentarios": int(post_object.comments),
                        "data_postagem": str(self.dataHandle.getDateFormatted(str(post_object.date))),
                        "localizacao": post_object.location,
                        "tipo_midia": "imagem" if post_object.typename == "GraphImage" else ("video" if post_object.typename == "GraphVideo" else "imagem"),
                        "identificador_midia": post_object.video_url if post_object.typename == "GraphVideo" else post_object.url,
                        "diretorio_midia": (str(self.filepath_medias)+str(post_object.shortcode)),
                        "tipo_documento": self.document_type,
                        "identificador_coleta": str(post_object.owner_username) if identificador_coleta is None else identificador_coleta
                        }

        return(post_document)


    def downloadPostMedia(self, post_id, media_url):
        has_error = False
        error_document = None
        try:
            media_filename = str(self.filename_output+str(post_id))

            if (os.path.exists(media_filename) is False):
                self.instaloaderInstance.download_pic(filename=media_filename, url=media_url, mtime=datetime.now())

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            has_error = True
            error_document = self.__getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)

        finally:
            return (has_error, error_document)



    def __getCommentDocument(self, post_id, comment_obj):
        comment_document = None
        try:
            comment_document = {"identificador": str(comment_obj.id),
                                "identificador_post": str(post_id),
                                "identificador_usuario": str(comment_obj.owner.userid),
                                "nome_do_usuario": str(comment_obj.owner.username),
                                "data_comentario": str(comment_obj.created_at_utc),
                                "texto": comment_obj.text,
                                "numero_likes": int(comment_obj.likes_count),
                                "tipo_documento": self.document_type}

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nErro: ', e, '\tDetalhes: ', exc_type, fname, exc_tb.tb_lineno, '\tData e hora: ', datetime.now(),
                  flush=True)
        finally:
            return(comment_document)


    def collectComments(self, post_id, comments_by_post_limit, line_debug_number = 1000):
        error_document = None
        has_error = False

        try:
            post = self.instaloaderClass.Post.from_shortcode(self.instaloaderInstance.context, post_id)

            comments = post.get_comments()

            inserted_comments = 0

            for comment in comments:
                inserted_comments += 1
                if inserted_comments % line_debug_number == 0:
                    print("\t\tColetando comentario numero {}".format(inserted_comments), '\tDatetime: ',
                          datetime.now(), flush=True)

                document = self.__getCommentDocument(post_id=post_id, comment_obj=comment)

                self.dataHandle.persistData(filename_output=self.filename_output,
                                            document_list=[document],
                                            operation_type="a")

                if (comments_by_post_limit is not None and inserted_comments > comments_by_post_limit):
                    # print("\tFinalizando coleta de comentarios do post {}. Limite de comentarios atingido.")
                    break

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error_document = self.__getErrorDocument(exception_obj=e, exc_type=exc_type, exc_tb=exc_tb)
            has_error = True

        finally:
            return (has_error, error_document)