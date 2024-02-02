#encoding: utf-8
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import openpyxl
from ajustar_planilha import ajustar_colunas, ajustar_bordas

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
df.to_html('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\CHATBOT\\Banco de dados Bot\\Dados CONAB.html', index=False)
    
#FAZ AS ALTERAÇÕES ESTRUTURAIS DA PLANILHA
wb_conab = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\Dados CONAB.xlsx")  
ws_conab = wb_conab.active

ajustar_colunas(ws_conab)
ajustar_bordas(wb_conab)
wb_conab.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\conab\\Dados CONAB.xlsx")