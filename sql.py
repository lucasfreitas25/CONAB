from ETL import dataframe_armazem, dataframe_custo_prod, dataframe_frete, dataframe_levantamento_graos, dataframe_preco_minimo, dataframe_precos_mes_muni, dataframe_serie_hist_graos
import psycopg2
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO public, conab')
    
    armazens = """
    CREATE TABLE IF NOT EXISTS conab.armazem (
    identificacao_armazem TEXT,
    dsc_especie_armazem TEXT,
    dsc_tipo_armazem TEXT,
    dsc_tipo_entidade TEXT,
    dsc_tipo_pessoa TEXT,
    nom_municipio TEXT,
    cod_ibge INTEGER,
    uf TEXT,
    qtd_capacidade_estatica_t NUMERIC,
    qtd_capacidade_expedicao_t NUMERIC,
    qtd_capacidade_recepcao_t NUMERIC,
    latitude NUMERIC,  
    longitude NUMERIC,
    nome_armazenador TEXT,
    endereco TEXT,
    email TEXT);
    """
    
    custo_prod = """
    CREATE TABLE IF NOT EXISTS conab.custo_prod (
    empreendimento TEXT,
    ano_mes INTEGER,
    produto TEXT,
    id_produto INTEGER,
    safra TEXT,
    uf TEXT,
    municipio TEXT,
    cod_ibge INTEGER,
    unidade_comercializacao TEXT,
    vlr_custo_variavel_ha NUMERIC,
    vlr_custo_variavel_unidade NUMERIC,
    vlr_custo_fixo_ha NUMERIC,
    vlr_custo_fixo_unidade NUMERIC,
    vlr_renda_fator_ha NUMERIC,
    vlr_renda_fator_unidade NUMERIC,
    data_ocorrencia DATE
    );
    """
    
    frete = """
    CREATE TABLE IF NOT EXISTS conab.frete (
    dsc_fonte TEXT,
    municipio_origem TEXT,
    cod_ibge_origem INTEGER,
    uf_origem TEXT,
    municipio_destino TEXT,
    cod_ibge_destino INTEGER,
    uf_destino TEXT,
    distancia_km INTEGER,
    valor_frete_tonelada NUMERIC,
    valor_tonelada_km NUMERIC,
    data_ocorrencia DATE
    );

    """
    
    lev_graos = """
    CREATE TABLE IF NOT EXISTS conab.levantamento_graos (
    safra TEXT,
    uf TEXT,
    produto TEXT,
    id_produto INTEGER,
    id_levantamento INTEGER,
    dsc_levantamento TEXT,
    area_plantada_mil_ha NUMERIC,
    producao_mil_t NUMERIC,
    produtividade_mil_ha_mil_t NUMERIC,
    ano_inicio_safra DATE,
    ano_fim_safra DATE,
    periodo TEXT
    );
    """
    
    preco_minimo = """
    CREATE TABLE IF NOT EXISTS conab.preco_minimo (
    descricao_produto_preco_minimo TEXT,
    id_produto INTEGER,
    uf TEXT,
    regionalizacao TEXT,
    ano_inicio_vigencia INTEGER,
    mes_inicio_vigencia INTEGER,
    ano_termino_vigencia INTEGER,
    mes_termino_vigencia INTEGER,
    preco NUMERIC,
    dsc_unidade_comercializacao TEXT,
    nome_normativo TEXT,
    url TEXT
    );
    """
    
    precos_mes_muni = """
    CREATE TABLE IF NOT EXISTS conab.precos_mensal_municipio (
    produto TEXT,
    classificao_produto TEXT,
    id_produto INTEGER,
    nom_municipio TEXT,
    cod_ibge INTEGER,
    uf TEXT,
    regiao TEXT,
    dsc_nivel_comercializacao TEXT,
    valor_produto_kg NUMERIC,
    data_ocorrencia DATE);
    """
    
    serie_hist_graos = """
    CREATE TABLE IF NOT EXISTS conab.serie_hist_graos (
    dsc_safra_previsao TEXT,
    uf TEXT,
    produto TEXT,
    id_produto INTEGER,
    area_plantada_mil_ha NUMERIC,
    producao_mil_t NUMERIC,
    produtividade_mil_ha_mil_t NUMERIC,
    ano_inicio_safra DATE,
    ano_fim_safra DATE,
    periodo TEXT
    );
    """
    
    cur.execute(armazens)
    cur.execute(lev_graos)
    cur.execute(frete)
    cur.execute(precos_mes_muni)
    cur.execute(preco_minimo)
    cur.execute(serie_hist_graos)
    cur.execute(custo_prod)
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'armazem'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.armazem;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'levantamento_graos'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.levantamento_graos;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'frete'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.frete;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'preco_minimo'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.preco_minimo;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'serie_hist_graos'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.serie_hist_graos;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'custo_prod'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.custo_prod;')
        
    cur.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'conab' 
            AND table_name = 'Precos_mensal_municipio'
        );
    ''')
    if cur.fetchone()[0]:
        cur.execute('TRUNCATE TABLE conab.Precos_mensal_municipio;')
    
    inserindo_armazem = '''
    INSERT INTO conab.armazem (identificacao_armazem, dsc_especie_armazem, dsc_tipo_armazem, dsc_tipo_entidade, dsc_tipo_pessoa, nom_municipio, cod_ibge, uf, qtd_capacidade_estatica_t, qtd_capacidade_expedicao_t, qtd_capacidade_recepcao_t, latitude, longitude, nome_armazenador, endereco, email)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_armazem.iterrows():
            dados = (
                i['identificacao_armazem'],
                i['dsc_especie_armazem'],
                i['dsc_tipo_armazem'],
                i['dsc_tipo_entidade'],
                i['dsc_tipo_pessoa'],
                i['nom_municipio'],
                i['cod_ibge'],
                i['uf'],
                i['qtd_capacidade_estatica_t'],
                i['qtd_capacidade_expedicao_t'],
                i['qtd_capacidade_recepcao_t'],
                i['latitude'],
                i['longitude'],
                i['nome_armazenador'],
                i['endereco'],
                i['email']
            )
            cur.execute(inserindo_armazem, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()

    inserindo_custo_prod = '''
    INSERT INTO conab.custo_prod (empreendimento, ano_mes, produto, id_produto, safra, uf, municipio, cod_ibge, unidade_comercializacao, vlr_custo_variavel_ha, vlr_custo_variavel_unidade, vlr_custo_fixo_ha, vlr_custo_fixo_unidade, vlr_renda_fator_ha, vlr_renda_fator_unidade, data_ocorrencia)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_custo_prod.iterrows():
            dados = (
                i['empreendimento'],
                i['ano_mes'],
                i['produto'],
                i['id_produto'],
                i['safra'],
                i['uf'],
                i['municipio'],
                i['cod_ibge'],
                i['unidade_comercializacao'],
                i['vlr_custo_variavel_ha'],
                i['vlr_custo_variavel_unidade'],
                i['vlr_custo_fixo_ha'],
                i['vlr_custo_fixo_unidade'],
                i['vlr_renda_fator_ha'],
                i['vlr_renda_fator_unidade'],
                i['data_ocorrencia']
            )
            cur.execute(inserindo_custo_prod, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()

    
    inserindo_frete = '''
    INSERT INTO conab.frete (dsc_fonte, municipio_origem, cod_ibge_origem, uf_origem, municipio_destino, cod_ibge_destino, uf_destino, distancia_km, valor_frete_tonelada, valor_tonelada_km, data_ocorrencia)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        for idx, i in dataframe_frete.iterrows():
            dados = (
                i['dsc_fonte'],
                i['municipio_origem'],
                i['cod_ibge_origem'],
                i['uf_origem'],
                i['municipio_destino'],
                i['cod_ibge_destino'],
                i['uf_destino'],
                i['distancia_km'],
                i['valor_frete_tonelada'],
                i['valor_tonelada_km'],
                i['data_ocorrencia']
            )
            cur.execute(inserindo_frete, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()
        
        
    inserindo_levantamento_graos = '''
    INSERT INTO conab.levantamento_graos (safra, uf, produto, id_produto, id_levantamento, dsc_levantamento, area_plantada_mil_ha, producao_mil_t, produtividade_mil_ha_mil_t, ano_inicio_safra, ano_fim_safra, periodo)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_levantamento_graos.iterrows():
            dados = (
                i['safra'],
                i['uf'],
                i['produto'],
                i['id_produto'],
                i['id_levantamento'],
                i['dsc_levantamento'],
                i['area_plantada_mil_ha'],
                i['producao_mil_t'],
                i['produtividade_mil_ha_mil_t'],
                i['ano_inicio_safra'],
                i['ano_fim_safra'],
                i['ano_agricola']
            )
            cur.execute(inserindo_levantamento_graos, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()
    
    inserindo_preco_minimo = '''
    INSERT INTO conab.preco_minimo (descricao_produto_preco_minimo, id_produto, uf, regionalizacao, ano_inicio_vigencia, mes_inicio_vigencia, ano_termino_vigencia, mes_termino_vigencia, preco, dsc_unidade_comercializacao, nome_normativo, url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_preco_minimo.iterrows():
            dados = (
                i['descricao_produto_preco_minimo'],
                i['id_produto'],
                i['uf'],
                i['regionalizacao'],
                i['ano_inicio_vigencia'],
                i['mes_incio_vigencia'],
                i['ano_termino_vigencia'],
                i['mes_termino_vigencia'],
                i['preco'],
                i['dsc_unidade_comercializacao'],
                i['nome_normativo'],
                i['url']
            )
            cur.execute(inserindo_preco_minimo, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()

    inserindo_precos_mes_muni = '''
    INSERT INTO conab.Precos_mensal_municipio (produto, classificao_produto, id_produto, nom_municipio, cod_ibge, uf, regiao, dsc_nivel_comercializacao, valor_produto_kg, data_ocorrencia)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_precos_mes_muni.iterrows():
            dados = (
                i['produto'],
                i['classificao_produto'],
                i['id_produto'],
                i['nom_municipio'],
                i['cod_ibge'],
                i['uf'],
                i['regiao'],
                i['dsc_nivel_comercializacao'],
                i['valor_produto_kg'],
                i['data_ocorrencia']
            )
            cur.execute(inserindo_precos_mes_muni, dados)
            conexao.commit()
    except psycopg2.errors as e:
        conexao.rollback()
        
    inserindo_serie_hist_graos = '''
    INSERT INTO conab.serie_hist_graos (dsc_safra_previsao, uf, produto, id_produto, area_plantada_mil_ha, producao_mil_t, produtividade_mil_ha_mil_t, ano_inicio_safra, ano_fim_safra, periodo)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe_serie_hist_graos.iterrows():
            dados = (
                i['dsc_safra_previsao'],
                i['uf'],
                i['produto'],
                i['id_produto'],
                i['area_plantada_mil_ha'],
                i['producao_mil_t'],
                i['produtividade_mil_ha_mil_t'],
                i['ano_inicio_safra'],
                i['ano_fim_safra'],
                i['ano_agricola']
            )
            cur.execute(inserindo_serie_hist_graos, dados)
            conexao.commit()
    except psycopg2.errors as e:
            conexao.rollback()
            
    cur.close()
    conexao.close()