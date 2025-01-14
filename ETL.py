import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import openpyxl
from ajustar_planilha import ajustar_colunas, ajustar_bordas
import numpy as np

def processar_ano_agricola(ano):
    if '/' in ano:
        ano_inicio, ano_fim = ano.split('/')
        if ano_inicio == '1999':
            digi_ano = '20'
        else:
            digi_ano = ano_inicio[:2]
        
        ano_fim = digi_ano + ano_fim
        return int(ano_inicio), int(ano_fim)
    else:
        ano_inicio, ano_fim = ano, ano
        return int(ano_inicio), int(ano_fim)

# EXTRAÇÃO DOS DADOS NO SITE
def extrair_site(url, arquivo):
    response = rq.get(url)
    conteudo_pagina = response.content.decode('utf-8')
    soup = BeautifulSoup(conteudo_pagina, 'html.parser')
    dados_texto = soup.get_text()
    dados_texto = '\n'.join(line.strip() for line in dados_texto.splitlines() if line.strip())
    with open(f'C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\{arquivo}.txt', 'w', encoding='utf-8') as arquivo:
        arquivo.write(dados_texto)


extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/ArmazensCadastrados.txt', "ArmazensCadastrados")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosMensalMunicipio.txt', "PrecosMensalMunicipio")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecoMinimo.txt', "PrecoMinimo")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/CustoProducao.txt', "CustoProducao")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoGraos.txt', "LevantamentoGraos")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt', "SerieHistoricaGraos")
extrair_site('https://portaldeinformacoes.conab.gov.br/downloads/arquivos/Frete.txt', "Frete")

substituicoes = {
    '00-18-18': 'Fertilizante NPK 00-18-18',
    '00-20-20': 'Fertilizante NPK 00-20-20',
    '04-30-05': 'Fertilizante NPK 04-30-05',
    '05-25-15': 'Fertilizante NPK 05-25-15',
    '08-16-16': 'Fertilizante NPK 08-16-16',
    '08-20-20': 'Fertilizante NPK 08-20-20',
    '20-00-20': 'Fertilizante NPK 20-00-20',
    '25-00-25': 'Fertilizante NPK 25-00-25',
    '30-00-20': 'Fertilizante NPK 30-00-20',
    '08-20-20 + ZN': 'Fertilizante NPK 08-20-20 + ZN',
    '2,4-D': 'Herbicida 2,4-D'
}

def tratamento_dados(local_arquivo, local_pasta, nome_dados):

    df = pd.read_csv(local_arquivo, sep=';', encoding='utf-8')

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        
    if nome_dados == 'PrecoMinimo':
        df['descricao_produto_preco_minimo'] = np.where(df['id_produto'] == 4457, 'FÉCULA', df['descricao_produto_preco_minimo'])
        df['nome_normativo'] = df['nome_normativo'].replace("","NÃO IDENTIFICADO")
        df['url'] = df['url'].fillna("NÃO INFORMADO")
        df['url'] = df['url'].replace("NI","NÃO IDENTIFICADO")
                    
    if nome_dados in ['PrecosMensalMunicipio', 'CustoProducao', 'Frete']:
    
        df['data_ocorrencia'] = pd.to_datetime('01/' + df['mes'].astype(str) + '/' + df['ano'].astype(str), format='%d/%m/%Y')
        df['data_ocorrencia'] = df['data_ocorrencia'].dt.date
        df['data_ocorrencia'] = pd.to_datetime(df['data_ocorrencia'], errors='coerce')
        df = df.drop(columns=['mes', 'ano'])
    
    if nome_dados == 'Frete':
        df[['valor_frete_tonelada', 'valor_tonelada_km']] = df[['valor_frete_tonelada', 'valor_tonelada_km']].replace(',', '.', regex=True).astype(float)

    if nome_dados == 'SerieHistoricaGraos':      
        df[['ano_inicio_safra', 'ano_fim_safra']] = df['ano_agricola'].apply(lambda x: processar_ano_agricola(x)).apply(pd.Series)      
        df['ano_inicio_safra'] = pd.to_datetime('01/01/' + df['ano_inicio_safra'].astype(str), format='%d/%m/%Y')
        df['ano_fim_safra'] = pd.to_datetime('31/12/' + df['ano_fim_safra'].astype(str), format='%d/%m/%Y') 
        # df = df.drop(columns=['ano_agricola'])
        
    if nome_dados == 'LevantamentoGraos':

        df[['ano_inicio_safra', 'ano_fim_safra']] = df['ano_agricola'].apply(lambda x: processar_ano_agricola(x)).apply(pd.Series)
        df['ano_inicio_safra'] = pd.to_datetime('01/01/' + df['ano_inicio_safra'].astype(str), format='%d/%m/%Y')
        df['ano_fim_safra'] = pd.to_datetime('31/12/' + df['ano_fim_safra'].astype(str), format='%d/%m/%Y')
        # df = df.drop(columns=['ano_agricola'])
        
    if nome_dados == 'Armazens_Cadastrados':
        df['email'] = df['email'].fillna("NÃO IDENTIFICADO")
        df['qtd_capacidade_estatica(t)'] = df['qtd_capacidade_estatica(t)'].str.replace(',', '.').astype(float)
        df.rename(columns={'qtd_capacidade_estatica(t)': 'qtd_capacidade_estatica_t','qtd_capacidade_expedicao(t)': 'qtd_capacidade_expedicao_t','qtd_capacidade_recepcao(t)': 'qtd_capacidade_recepcao_t'
        }, inplace=True)
        
    if nome_dados == 'PrecosMensalMunicipio':
        df['valor_produto_kg'] = df['valor_produto_kg'].str.replace(',', '.').astype(float)
        for chave in substituicoes:
            df['produto'] = df['produto'].replace(chave, substituicoes[chave])

    df.to_excel(f'{local_pasta}\\Dados {nome_dados}.xlsx', index=False)
        
    wb_conab = openpyxl.load_workbook(f'{local_pasta}\\Dados {nome_dados}.xlsx')  
    ws_conab = wb_conab.active

    ajustar_colunas(ws_conab)
    ajustar_bordas(wb_conab)
    wb_conab.save(f'{local_pasta}\\Dados {nome_dados}.xlsx')
    return df

local_pasta_tratada = 'C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas tratadas'

dataframe_armazem = tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\ArmazensCadastrados.txt', local_pasta_tratada, "Armazens_Cadastrados")
dataframe_precos_mes_muni  =tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\PrecosMensalMunicipio.txt', local_pasta_tratada, "PrecosMensalMunicipio")
dataframe_preco_minimo  =tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\PrecoMinimo.txt', local_pasta_tratada, "PrecoMinimo")
dataframe_custo_prod  =tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\CustoProducao.txt', local_pasta_tratada, "CustoProducao")
dataframe_levantamento_graos  =tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\LevantamentoGraos.txt', local_pasta_tratada, "LevantamentoGraos")
dataframe_serie_hist_graos  =tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\SerieHistoricaGraos.txt', local_pasta_tratada, "SerieHistoricaGraos")
dataframe_frete  = tratamento_dados('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\CONAB\\Planilhas em txt\\Frete.txt', local_pasta_tratada, "Frete")

if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()