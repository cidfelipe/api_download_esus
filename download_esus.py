## importar as bibliotecas
## Utilizar a versão 7.13.1 da bibliteca elasticsearc nas versão 8 a forma de 

import pandas as pd
from elasticsearch7 import Elasticsearch
import elasticsearch7.helpers
import csv
import io
from datetime import datetime

my_index = "my_index"
pwd = "my_password"
user = "my_user"

columns = ['id', 'idCollection', 'dataNascimento', 'dataNotificacao', 'dataInicioSintomas',
       'dataTeste', 'estrangeiro', 'profissionalSaude', 'profissionalSeguranca', 'cbo', 'cpf', 
       'cns', 'nomeCompleto', 'nomeMae', 'paisOrigem', 'sexo', 'racaCor', 'passaporte', 'cep',
       'logradouro', 'numero', 'complemento', 'bairro', 'estado', 'municipio', 'telefoneContato',
       'telefone', 'sintomas', 'outrosSintomas', 'condicoes', 'estadoTeste', 'tipoTeste', 
       'resultadoTeste', 'numeroNotificacao', '_p_usuario', 'cnes', 'estadoNotificacao', 'municipioNotificacao',
       'origem', '_created_at', '_updated_at', '_p_usuarioAlteracao', 'classificacaoFinal', 'dataEncerramento',
       'evolucaoCaso', 'desnormalizarNome', 'nomeCompletoDesnormalizado', 'estadoIBGE', 'estadoNotificacaoIBGE',
       'municipioIBGE', 'municipioNotificacaoIBGE', 'notificadorCpf', 'notificadorEmail', 'notificadorNome',
       'notificadorCNPJ', 'idade', '@timestamp', 'descricaoRacaCor', 'etnia', 'rpa', 'idOrigem',
       'testeSorologico', 'dataTesteSorologico', 'tipoTesteSorologico', 'resultadoTesteSorologicoIgA',
       'resultadoTesteSorologicoIgG', 'resultadoTesteSorologicoIgM', 'resultadoTesteSorologicoTotais', 
       'contemComunidadeTradicional', 'comunidadeTradicional', 'testes', 'estrategiaCovid', 'codigoEstrategiaCovid', 
       'buscaAtivaAssintomatico', 'outroBuscaAtivaAssintomatico', 'triagemPopulacaoEspecifica', 
       'outroTriagemPopulacaoEspecifica', 'localRealizacaoTestagem', 'outroLocalRealizacaoTestagem',
       'codigoTriagemPopulacaoEspecifica', 'codigoLocalRealizacaoTestagem', 'recebeuVacina', 'dosesVacina', 
       'laboratorioPrimeiraDose', 'laboratorioSegundaDose', 'lotePrimeiraDose', 'loteSegundaDose', 'registroAtual']



#index = user.replace('svs-esusve','esus-notifica') # user 



print('Início: ' + my_index)
protocol = 'https'
url = protocol + '://' + user + ':' + pwd +'@elasticsearch-saps.saude.gov.br' + '/'
es = Elasticsearch([url], send_get_body_as='POST', timeout=500)
#print(url)

#opções de consulta, todos os dados ou por data
#body={"query": {"match_all": {}}}
#data_2020={"query": {"bool": {"must": {"match_all": {}}, "filter": { "range": {"dataInicioSintomas": {"gte": "2020-01-01T00:00:00", "lte": "2020-12-31T23:59:59"}}}}}}
data_2021={"query": {"bool": {"must": {"match_all": {}}, "filter": { "range": {"dataInicioSintomas": {"gte": "2021-01-01T00:00:00", "lte": "2021-12-31T23:59:59"}}}}}}
#data_2021_1sem={"query": {"bool": {"must": {"match_all": {}}, "filter": { "range": {"dataInicioSintomas": {"gte": "2021-01-01T00:00:00", "lte": "2021-06-30T23:59:59"}}}}}}
#data_2021_2sem={"query": {"bool": {"must": {"match_all": {}}, "filter": { "range": {"dataInicioSintomas": {"gte": "2021-07-01T00:00:00", "lte": "2021-12-31T23:59:59"}}}}}}
#data_2022={"query": {"bool": {"must": {"match_all": {}}, "filter": { "range": {"dataInicioSintomas": {"gte": "2022-01-01T00:00:00", "lte": "now"}}}}}}

#mudar no campo abaixo utilizando uma das tres opções acima no atributo query.
#a opção body traz todos os dados do inicio até a data atual
#a opção data_2020 traz todos os registros com o campo '_updated_at' com data entre 01/01/2020 a 31/12/2020
#a opção data_2021 traz todos os registros com o campo '_updated_at' com data entre 01/01/2021 até a data atual
#mudar aqui pra consulta desejada
results = elasticsearch7.helpers.scan(es, query=data_2021, index=my_index) #data_2020

start_time = datetime.now()
#mudar da linha a baixo, após a barra da data 20220110/2021_ ou 2022_
with io.open('esus_2021_'+ my_index + '.csv', "w", encoding="utf-8", newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=columns, delimiter=';', extrasaction='ignore')
    writer.writeheader()
    mid_time = datetime.now()
    i = 0
    printVal = 100000
    for document in results:
        writer.writerow(document['_source'])
        i += 1
        horafor =  datetime.now()
        if ((i % printVal) == 0) and (i >= printVal):
            print("registros escritos: ", i, " ", horafor)
end_time = datetime.now()
print('Tempo Gravação: {}'.format(end_time - start_time))

#df = pd.DataFrame.from_dict([document['_source'] for document in results])

print("Numero de casos:",i,"de",es.count(index=my_index)['count'])
print("Final: " + my_index)
#df.to_csv('./dadosESUS//'+index+'.csv', sep = ';', encoding='utf-8-sig', index = False)

    
print("Finished here!")
