# Importa os módulos necessários para operações HTTP, manipulação de JSON e
# processamento de dados em formato de planilha.

# Módulo para realizar solicitações HTTP e HTTPS a servidores web.
import http.client

# Módulo para codificar e decodificar dados em formato JSON, uma forma
# leve de troca de dados.
import json

# Biblioteca que oferece estruturas de dados e ferramentas de análise de
# dados, útil para manipular tabelas.
import pandas as pd

def limpar_cnpj(cnpj):
    
    """
    Esta função é responsável por limpar a string de CNPJ, removendo quaisquer
            caracteres que não sejam dígitos numéricos.
    Isso é essencial porque o CNPJ pode ser fornecido em formatos diversos, com 
            pontos, traços ou barras,
            que não são aceitos em solicitações de API que esperam apenas a sequência numérica.

    Parâmetros:
    cnpj (str): A string do CNPJ que pode conter formatos diversos, incluindo dígitos, 
            pontos, traços e barras.

    Retorna:
    str: Uma string contendo apenas os dígitos numéricos do CNPJ, adequada para 
            uso em URLs ou consultas de banco de dados.
    """
    
    # A função `filter` é utilizada aqui para iterar sobre cada caractere da string 'cnpj'.
    # A função `str.isdigit` é passada como argumento para `filter`, que aplica esta
            # função a cada caractere.
    # `str.isdigit` retorna True se o caractere for um dígito numérico, o que significa
            # que ele é retido na sequência final.
    # Caso contrário, caracteres como pontos, traços e espaços, que retornariam False,
            # são removidos da sequência final.
    # O resultado de `filter`, que é um iterador sobre os caracteres que são dígitos, é
            # convertido de volta em uma string usando `join`.
    # A função `join` é chamada em uma string vazia '', que serve como separador entre os
            # caracteres no resultado final.
    # Isso significa que todos os caracteres numéricos são concatenados sem nenhum
            # separador entre eles.
    return ''.join(filter(str.isdigit, cnpj))


def obter_dados_empresa_por_cnpj(cnpj):
    
    """
    Esta função faz uma consulta à API ReceitaWS para obter informações 
                sobre uma empresa, usando o CNPJ como chave de pesquisa.
    O processo envolve limpar o CNPJ para remover caracteres não numéricos, 
                enviar uma requisição HTTP GET e interpretar a resposta.
    
    Parâmetros:
    cnpj (str): CNPJ da empresa a ser consultada, podendo inicialmente 
                incluir caracteres como traços ou pontos.
    
    Retorna:
    dict: Um dicionário contendo as informações da empresa recuperadas 
                da API, ou None se ocorrer algum erro, como uma resposta HTTP não 
                bem-sucedida ou problemas na decodificação do JSON.
    """
    
    # Primeiro, a função 'limpar_cnpj' é chamada para remover quaisquer
                # caracteres não numéricos do CNPJ fornecido.
    # Isso é crucial porque a API espera um CNPJ apenas com dígitos numéricos.
    cnpj = limpar_cnpj(cnpj)
    
    # Cria uma conexão HTTPS com o domínio 'www.receitaws.com.br'.
    # O protocolo HTTPS é usado aqui para garantir a segurança dos dados
    # transmitidos através da Internet.
    conexao = http.client.HTTPSConnection("www.receitaws.com.br")
    
    # Envia uma requisição GET para o servidor da API, especificamente ao endpoint que
            # aceita CNPJ como parâmetro.
    # O método `GET` é utilizado para recuperar dados do servidor sem afetar o estado
            # do mesmo, o que é ideal para consultas.
    # A URL para a requisição é construída usando f-strings, que permite a inserção
            # dinâmica de variáveis dentro de strings.
    # O endpoint `/v1/cnpj/{cnpj}` é especificamente projetado para aceitar um CNPJ como parte da URL.
    # Aqui, `{cnpj}` na URL é substituído pelo valor da variável `cnpj`, que contém
            # o CNPJ da empresa a ser consultado.
    # Este CNPJ é inserido diretamente na URL, seguindo as especificações da API, garantindo
            # que o CNPJ correto seja utilizado na consulta.
    # A função `request` do objeto `conexao`, que é uma instância de `http.client.HTTPSConnection`,
            # é chamada com dois argumentos:
    # o método HTTP 'GET' e a URL formatada.
    # Esta chamada inicia o processo de envio da requisição HTTP ao servidor API, esperando por
            # uma resposta que contém os dados da empresa.
    conexao.request("GET", f"/v1/cnpj/{cnpj}")

    
    # Recebe a resposta do servidor após o envio da requisição.
    # O objeto 'resposta' contém tanto o status do HTTP quanto o corpo da resposta.
    resposta = conexao.getresponse()
    
    # Para fins de depuração e controle de fluxo, o status da resposta HTTP é impresso.
    # Isso ajuda a verificar se a requisição foi bem-sucedida.
    print(f"Processando CNPJ {cnpj}: Status {resposta.status}")
    
    # Verifica se o status HTTP da resposta não é 200 (OK).
    # Qualquer status diferente de 200 indica que algo deu errado com a
    # requisição, e a função então encerra prematuramente.
    if resposta.status != 200:

        # A conexão é fechada para liberar recursos.
        conexao.close()

        # A função retorna None para indicar que a requisição falhou.
        return None  
    
    # Se o status for 200, o conteúdo da resposta é lido.
    # O método 'read' obtém o corpo da resposta, que é um conjunto de bytes.
    dados = resposta.read()
    
    # Após a leitura dos dados, a conexão é fechada.
    conexao.close()
    
    # Tenta decodificar os bytes para uma string formatada em UTF-8 e então
            # carrega essa string como um dicionário JSON.
    # Esse passo é crucial porque transforma a string JSON em um objeto Python
            # que pode ser facilmente manipulado.
    empresa = json.loads(dados.decode("utf-8"))
    
    # Verifica se a resposta contém a chave 'status' com o valor 'ERROR', indicando um
            # erro na busca de dados.
    # Se um erro é encontrado, uma mensagem correspondente é impressa e a função retorna None.
    if "status" in empresa and empresa["status"] == "ERROR":
        print(f"Erro ao buscar dados para o CNPJ {cnpj}: {empresa.get('message', 'Sem mensagem de erro')}")
        return None
    
    # Se tudo ocorrer bem, o dicionário com as informações da empresa é retornado.
    return empresa


def formatar_dados(empresa):
    
    """
    Esta função é projetada para extrair e formatar campos específicos 
                dos dados de uma empresa obtidos via API.
    Ela simplifica o dicionário de entrada, selecionando apenas os 
                campos relevantes para uso posterior, como em 
                relatórios ou interfaces de usuário.

    Parâmetros:
    empresa (dict): Dicionário contendo todos os dados da 
                empresa como retornado pela API.

    Retorna:
    dict: Dicionário contendo apenas os campos selecionados e 
                formatados da empresa.
    """
    
    # Lista de campos específicos que são de interesse para a
                # aplicação ou o processo de negócio.
    # Esses campos foram escolhidos porque são comuns em consultas de
                # dados de empresas e úteis para análises básicas ou contato.
    campos_interesse = ['cnpj', 'nome', 'telefone', 'email', 'logradouro', 'bairro', 'municipio', 'uf', 'cep', 'atividade_principal']
    
    # Cria um novo dicionário que irá conter apenas os campos especificados.
    # Utiliza um dicionário por compreensão para iterar sobre a lista de campos de interesse.
    # Para cada campo, o método `get` é utilizado para buscar o valor correspondente no
            # dicionário original `empresa`.
    # Se o campo não estiver presente no dicionário `empresa`, o método `get` retorna
            # uma string vazia como padrão (''),
    # o que ajuda a manter a consistência do dicionário de saída e evita erros de chave não encontrada.
    dados_formatados = {campo: empresa.get(campo, '') for campo in campos_interesse}
    
    # Verifica se a chave 'atividade_principal' está presente no dicionário e
            # se ela contém algum valor.
    # A chave 'atividade_principal' geralmente contém uma lista de atividades, onde
            # cada atividade é um dicionário.
    # O interesse é extrair apenas o texto da primeira atividade principal listada, se disponível.
    # Acessa o primeiro elemento da lista (índice [0]) e busca a chave 'text' dentro deste dicionário.
    # Se a chave 'text' não estiver disponível, retorna uma string vazia como valor padrão.
    if 'atividade_principal' in empresa and empresa['atividade_principal']:
        dados_formatados['atividade_principal'] = empresa['atividade_principal'][0].get('text', '')
    
    # Retorna o dicionário formatado contendo apenas os campos de interesse com seus
            # valores extraídos ou padrões aplicados.
    return dados_formatados


def salvar_dados_empresa_excel(resultados, nome_arquivo, nome_aba="Dados"):
    
    """
    Esta função utiliza a biblioteca pandas para salvar um DataFrame de 
                dados das empresas em uma planilha Excel existente.
    Ela pode adicionar dados em uma aba existente ou criar uma 
                nova aba se necessário.

    Parâmetros:
    resultados (DataFrame): DataFrame contendo os dados das empresas a serem salvos.
                nome_arquivo (str): Caminho do arquivo Excel onde os 
                dados serão salvos.
    nome_aba (str): Nome da aba onde os dados serão adicionados ou 
                criados. O nome padrão da aba é 'Dados'.

    Retorno:
    None: A função não retorna nenhum valor, mas salva os dados 
                diretamente no arquivo Excel especificado.
    """
    
    # Utiliza o ExcelWriter para abrir o arquivo Excel especificado.
    # O ExcelWriter é uma ferramenta do pandas que facilita a escrita de
                # dados em múltiplos formatos de arquivo Excel sem
                # destruir os dados existentes.
    # Parâmetro 'engine' define que 'openpyxl' é usado como
                # mecanismo para trabalhar com arquivos xlsx.
    # Parâmetro 'mode' como 'a' permite anexar aos arquivos
                # existentes sem apagar o conteúdo anterior.
    # Parâmetro 'if_sheet_exists' como 'overlay' permite sobrepor dados na aba especificada se ela já existir.
    with pd.ExcelWriter(nome_arquivo, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        
        try:
            
            # Tenta acessar a aba com o nome especificado dentro do arquivo Excel.
                        # Se a aba existir, 'worksheet' armazena essa aba.
            worksheet = writer.book[nome_aba]
            
            # Determina a última linha preenchida na aba existente para
                        # saber onde começar a adicionar novos dados.
            start_row = worksheet.max_row
            
            # Adiciona os dados do DataFrame 'resultados' na aba existente,
                        # começando após a última linha preenchida.
            # 'index=False' significa que os índices do DataFrame não
                        # serão escritos no Excel.
            # 'header=False' significa que os cabeçalhos das colunas também
                        # não serão escritos, pois se assume que a aba já possui cabeçalhos.
            resultados.to_excel(writer, sheet_name=nome_aba, index=False, header=False, startrow=start_row)
            
        except KeyError:
            
            # Se a aba especificada não existir no arquivo, um erro de chave é capturado.
            # Neste caso, cria-se uma nova aba com o nome especificado e
                        # adiciona-se os dados com cabeçalhos de coluna.
            resultados.to_excel(writer, sheet_name=nome_aba, index=False, header=True)
            
            # 'index=False' e 'header=True' significam que os dados serão
                        # escritos sem índices, mas com cabeçalhos de coluna,
                        # facilitando a identificação dos dados em uma nova aba.
            

# Define o caminho do arquivo Excel onde os dados serão salvos.
caminho_planilha = "CNPJ.xlsx"

# Carrega os dados de CNPJs da planilha, tratando a coluna CNPJ como string
        # para evitar perda de dígitos.
# A função 'read_excel' do pandas é usada para ler dados de uma planilha Excel.
# 'caminho_planilha' especifica o caminho do arquivo Excel que contém os dados.
# 'sheet_name' especifica a aba dentro do arquivo Excel de onde os dados devem ser lidos.
# 'dtype' especifica o tipo de dados de cada coluna; aqui, a coluna 'CNPJ' é tratada como string (str)
            # para preservar os zeros à esquerda e outros formatos numéricos que poderiam ser
# interpretados incorretamente como números inteiros.
planilha_cnpjs = pd.read_excel(caminho_planilha, sheet_name='CNPJ', dtype={'CNPJ': str})

# Inicializa uma lista vazia para armazenar os resultados formatados de
            # cada empresa consultada.
resultados = []

# Itera sobre os CNPJs da planilha, obtendo e formatando os dados de cada empresa.
# 'dropna()' é usado para eliminar quaisquer valores NaN que possam existir na
            # coluna 'CNPJ', garantindo que apenas CNPJs válidos sejam processados.
for cnpj in planilha_cnpjs['CNPJ'].dropna():
    
    # Imprime o CNPJ atual sendo processado para fins de depuração e
            # acompanhamento do progresso.
    print(f"Lendo CNPJ: {cnpj}")
    
    # Chama a função 'obter_dados_empresa_por_cnpj' para cada CNPJ,
            # convertendo o CNPJ para string por precaução,
            # embora já esteja definido como string pelo 'dtype' na leitura da planilha.
    dados_empresa = obter_dados_empresa_por_cnpj(str(cnpj))
    
    # Verifica se dados válidos foram retornados (i.e., 'dados_empresa' não é None).
    if dados_empresa:
        
        # Se dados válidos foram obtidos, chama a função 'formatar_dados' para
                    # estruturar os dados em um formato mais simples e uniforme.
        dados_formatados = formatar_dados(dados_empresa)
        
        # Adiciona os dados formatados à lista 'resultados'.
        resultados.append(dados_formatados)


# Verifica se a lista 'resultados' contém algum dicionário de dados formatados.
# 'resultados' é preenchida com dicionários se dados válidos foram
            # obtidos e formatados nas etapas anteriores.
if resultados:
    
    # Converte a lista de dicionários 'resultados' em um DataFrame do pandas.
    # Um DataFrame é uma estrutura de dados tabular muito usada em análise de
            # dados que facilita a manipulação, análise e visualização.
    # Cada dicionário na lista 'resultados' se torna uma linha no DataFrame, e
            # as chaves dos dicionários se tornam as colunas.
    resultados_df = pd.DataFrame(resultados)
    
    # Chama a função 'salvar_dados_empresa_excel' para gravar o
            # DataFrame 'resultados_df' no arquivo Excel especificado.
    # 'caminho_planilha' é o caminho do arquivo onde os dados serão salvos.
    # A função é responsável por adicionar os dados em uma aba específica
            # ou criar uma nova se necessário,
    # garantindo que os dados sejam persistidos de forma organizada e acessível.
    salvar_dados_empresa_excel(resultados_df, caminho_planilha)

# Após tentar salvar os dados, imprime uma mensagem indicando que os dados
            # das empresas foram salvos na planilha.
print("Dados das empresas foram salvos na planilha na aba 'Dados'.")
