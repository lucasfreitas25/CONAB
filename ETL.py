#encoding: utf-8
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import openpyxl


# EXTRAÇÃO DOS DADOS NO SITE

url_dados = 'https://portaldeinformacoes.conab.gov.br/downloads/arquivos/ArmazensCadastrados.txt'
response = rq.get(url_dados)
conteudo_pagina = response.text
soup = BeautifulSoup(conteudo_pagina, 'html.parser')
dados_texto = soup.get_text()

with open('conteudo_site.txt', 'w', encoding='latin-1') as arquivo:
    arquivo.write(dados_texto)

    
#TRATAMENTO DOS DADOS
df = pd.read_csv('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\conteudo_site.txt', sep=';', encoding='utf-8')

df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df.fillna(value=0, inplace=True)

df['qtd_capacidade_estatica(t)'] = df['qtd_capacidade_estatica(t)'].str.replace(',', '.').astype(float)

df.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\Dados CONAB.xlsx', index=False)

    
#FAZ AS ALTERAÇÕES ESTRUTURAIS DA PLANILHA
wb_conab = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\Dados CONAB.xlsx")  
ws_conab = wb_conab.active

colunas_para_ajustar = ['A' ,'B', 'C', 'D', 'E']
largura_desejada = 22

for coluna in colunas_para_ajustar:
    ws_conab.column_dimensions[coluna].width = largura_desejada

colunas_maiores = ['I', 'J', 'K']
largura_planejada = 27

for coluna in colunas_maiores:
    ws_conab.column_dimensions[coluna].width = largura_planejada
    
ws_conab.column_dimensions['F'].width = 37    
wb_conab.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\Dados CONAB.xlsx")