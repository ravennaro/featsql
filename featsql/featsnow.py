# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/03_creation_snowflake.ipynb.

# %% auto 0
__all__ = ['snow_query_janela_num', 'snow_query_join_num', 'snow_create_query_num', 'snow_query_janela_cat',
           'snow_create_query_cat', 'snow_query_agregada', 'snow_create_query_agregada']

# %% ../nbs/03_creation_snowflake.ipynb 4
def snow_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra):
    query_janela = ""
    lista_vars_join = ""
    lista_vars_janelas = ""

    for n in lista_janela:
        query_variaveis_numericas = ""
        for i in feat_num_lista:
            query_variaveis_numericas += f"""
             -- Criação de variáveis numéricas a partir da coluna {i} para a janela {n}
            SUM(COALESCE({tb_feat}.{i},0)) AS {i}_SUM_{n}M,
            MIN(COALESCE({tb_feat}.{i},0)) AS {i}_MIN_{n}M,
            MAX(COALESCE({tb_feat}.{i},0)) AS {i}_MAX_{n}M,
            AVG(COALESCE({tb_feat}.{i},0)) AS {i}_AVG_{n}M,
            MEDIAN(COALESCE({tb_feat}.{i},0)) AS {i}_MEDIAN_{n}M,
            """
            lista_vars_join += f"""
            tb_join.{i}_SUM_{n}M,
            tb_join.{i}_MIN_{n}M,
            tb_join.{i}_MAX_{n}M,
            tb_join.{i}_AVG_{n}M,
            tb_join.{i}_MEDIAN_{n}M,
            """
            lista_vars_janelas += f"""
            tb_janela_{n}M.{i}_SUM_{n}M,
            tb_janela_{n}M.{i}_MIN_{n}M,
            tb_janela_{n}M.{i}_MAX_{n}M,
            tb_janela_{n}M.{i}_AVG_{n}M,
            tb_janela_{n}M.{i}_MEDIAN_{n}M,
            """
        query_variaveis_numericas = query_variaveis_numericas.rstrip(', \n')

        query_janela += f"""
        -- Criação de variáveis de janela de {n}M
        tb_janela_{n}M AS (
            SELECT 
                tb_public.{id},
                tb_public.{safra_ref},
                {query_variaveis_numericas}
            FROM tb_public
            INNER JOIN {tb_feat} 
                ON  tb_public.{id} = {tb_feat}.{id}
                AND (DATEADD('month', {n} , TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD')) >=  tb_public.{safra_ref}) 
                AND (TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD') < tb_public.{safra_ref})
            GROUP BY tb_public.{id}, tb_public.{safra_ref}
        ),
        """
    
    lista_vars_join = lista_vars_join.rstrip(', \n')
    lista_vars_janelas = lista_vars_janelas.rstrip(', \n')
    return query_janela, lista_vars_join, lista_vars_janelas


# %% ../nbs/03_creation_snowflake.ipynb 5
def snow_query_join_num(lista_janela, id, safra_ref):
    query_join = ""
    for i in lista_janela:
        query_join += f"""
        LEFT JOIN tb_janela_{i}M
            ON tb_public.{id} = tb_janela_{i}M.{id}
            AND tb_public.{safra_ref} = tb_janela_{i}M.{safra_ref}
    """
    return query_join

# %% ../nbs/03_creation_snowflake.ipynb 6
def snow_create_query_num(tb_publico, # public table: contains the public, target and reference date
                            tb_feat, # feature table: table with columns that will be transformed into features
                            lista_janela, # time window list
                            feat_num_lista, # list of columns that will be transform into features  
                            id, # id column name
                            safra_ref, # reference date column name on public table
                            safra, # date column name on feature table
                            nome_arquivo=None, # name of the .sql file where the query is saved
                            status=False, # if True, it creates a table on database
                            table_name=None, # table name created
                            conn=None # Database connection 
                            ): 
    if status:
        # Apagando tabela se existir do banco de dados:
        cursor = conn.cursor()
        query_drop_table = f"""
-- Apaga tabela com o nome {table_name}
DROP TABLE IF EXISTS {table_name};
"""        
        # Executando query para apagar tabela se existir
        cursor.execute(query_drop_table)

        # Query de construção de tabela 
        query_create_table = f"""
-- Criar a tabela {table_name}
CREATE TABLE {table_name} AS
"""
      
    else:
        # Quando satus=False, a função não cria a tabela
        query_drop_table=""
        query_create_table=""


    query_janela, lista_vars_join, lista_vars_janelas = snow_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra)
    query_num = f"""
    {query_create_table}

    WITH 
    tb_public AS (
        SELECT 
            *
        FROM {tb_publico}
    ),
    {query_janela}

    tb_join AS (
        SELECT 
            tb_public.*,
            {lista_vars_janelas}

        FROM tb_public 
        {snow_query_join_num(lista_janela, id, safra_ref)}
    )
        
    SELECT 
        tb_join.{id},
        tb_join.{safra_ref},
        {lista_vars_join}
    FROM tb_join
    """

    if status:
        # Executando query de criação da tabela
        cursor.execute(query_num)
        cursor.close()
        
    # Salvando arquivo em file .sql:
    try: 
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(query_drop_table)
            arquivo.write(query_num)
        print(f'Complete query creation with {nome_arquivo} saved file')
    except:
        print("Complete query creation with no saved file.")
    return query_num

# %% ../nbs/03_creation_snowflake.ipynb 8
def snow_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra):
    vars_cat = ""
    query_janela_cat = ""
    query_join =""

    for n in lista_janela:      
        moda_feat = ""
        for i in feat_cat_lista:
            moda_feat += f"""
            MODE({i}) AS MODE_{i}_{n}M,"""
            vars_cat += f"""
        tb_janela_{n}M.MODE_{i}_{n}M,"""
        moda_feat = moda_feat.rstrip(', \n')  


        query_janela_cat += f"""
    tb_janela_{n}M AS (
        SELECT
            tb_public.{id},
            tb_public.{safra_ref},
            {moda_feat}           
            
        FROM tb_public
        LEFT JOIN {tb_feat}
            ON  tb_public.{id} = {tb_feat}.{id}
            AND (DATEADD('month', {n} , TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD')) >=  tb_public.{safra_ref}) 
            AND (TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD') < tb_public.{safra_ref})
        GROUP BY tb_public.{id}, tb_public.{safra_ref}
    ),
"""     
        query_join += f"""
    LEFT JOIN tb_janela_{n}M
        ON tb_public.{id} = JOIN tb_janela_{n}M.{id}
        AND tb_public.{safra_ref} = JOIN tb_janela_{n}M.{safra_ref}
"""


    vars_cat = vars_cat.rstrip(', \n')
    query_janela_cat = query_janela_cat.rstrip(', \n')
    vars_cat = vars_cat.rstrip(', \n')  
    return query_janela_cat, vars_cat, query_join

# %% ../nbs/03_creation_snowflake.ipynb 9
def snow_create_query_cat(tb_publico, # public table: contains the public, target and reference date
                            tb_feat, # feature table: table with columns that will be transformed into features
                            lista_janela, # time window list
                            feat_cat_lista, # list of columns that will be transform into features  
                            id, # id column name
                            safra_ref, # reference date column name on public table
                            safra, # date column name on feature table
                            nome_arquivo=None, # name of the .sql file where the query is saved
                            status=False, # if True, it creates a table on database
                            table_name=None, # table name created
                            conn=None # Database connection 
                            ):
    if status:
        # Apagando tabela se existir do banco de dados:
        cursor = conn.cursor()
        query_drop_table = f"""
-- Apaga tabela com o nome {table_name}
DROP TABLE IF EXISTS {table_name};
"""        
        # Executando query para apagar tabela se existir
        cursor.execute(query_drop_table)

        # Query de construção de tabela 
        query_create_table = f"""
-- Criar a tabela {table_name}
CREATE TABLE {table_name} AS
"""
      
    else:
        # Quando satus=False, a função não cria a tabela
        query_drop_table=""
        query_create_table=""
    query_janela_cat, lista_vars, query_join = snow_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra)



    query_num_cat = f"""
    {query_create_table}
    WITH 
    tb_public AS (
        SELECT 
            {id},
            {safra_ref}
        FROM {tb_publico}
    ),
    {query_janela_cat}

    SELECT 
        tb_public.{id},
        tb_public.{safra_ref},
        {lista_vars}
        
    FROM tb_public
    {query_join}
    """
    if status:
        # Executando query de criação da tabela
        cursor.execute(query_num_cat)
        cursor.close()
        
    # Salvando arquivo em file .sql:
    try: 
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(query_drop_table)
            arquivo.write(query_num_cat)
        print(f'Complete query creation with {nome_arquivo} saved file')
    except:
        print("Complete query creation with no saved file.")
        
    return query_num_cat

# %% ../nbs/03_creation_snowflake.ipynb 11
def snow_query_agregada(janela, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra):    
    
    table_name =f"tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M"
    vars=""


    query= f"""
-- Criação de variáveis agrupadas com janela de {janela}M
tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M as(
    SELECT
        tb_public.{id},
        tb_public.{safra_ref},
"""
    for feat_num in lista_feat_num:
        query +=f"""
        -- Criação de variáveis agrupadas a partir da coluna {feat_cat} e {feat_num} para a janela {janela}
        SUM(COALESCE({tb_feat}.{feat_num},0))  AS SUM_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        MAX(COALESCE({tb_feat}.{feat_num},0))  AS MAX_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        MIN(COALESCE({tb_feat}.{feat_num},0))  AS MIN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        AVG(COALESCE({tb_feat}.{feat_num},0))  AS AVG_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        MEDIAN(COALESCE({tb_feat}.{feat_num},0))  AS MEDIAN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
"""
        vars += f"""
        tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.SUM_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.MAX_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.MIN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.AVG_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
        tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.MEDIAN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
"""
        

    query = query.rstrip(', \n')
    
    query +=f"""
    FROM tb_public
    INNER JOIN {tb_feat}
        ON  tb_public.{id} = {tb_feat}.{id}
        AND (DATEADD('month', {janela} , TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD')) >=  tb_public.{safra_ref}) 
        AND (TO_DATE({tb_feat}.{safra}, 'YYYY-MM-DD') < tb_public.{safra_ref})
        AND {tb_feat}.{feat_cat} = '{feat_cat_valor}'
    GROUP BY tb_public.{id}, tb_public.{safra_ref}
),
"""
    return query, table_name, vars

# %% ../nbs/03_creation_snowflake.ipynb 12
def snow_create_query_agregada(tb_publico, # public table: contains the public, target and reference date
                                 tb_feat, # feature table: table with columns that will be transformed into features
                                 lista_janela, # time window list
                                 lista_feat_num, # list of numerical columns
                                 id, # id column name 
                                 safra_ref, # reference date column name on public table
                                 safra, # date column name on feature table
                                 feat_cat, # categorical column that will be aggregated
                                 lista_valor_agragador, # list of feat_cat values that will be aggregated into features
                                 nome_arquivo=None, # name of the .sql file where the query is saved
                                 status=False, # if True, it creates a table on database
                                 table_name=None, # table name created
                                 conn=None # Database connection 
                                 ):
    if status:
        # Apagando tabela se existir do banco de dados:
        cursor = conn.cursor()
        query_drop_table = f"""
-- Apaga tabela com o nome {table_name}
DROP TABLE IF EXISTS {table_name};
"""        
        # Executando query para apagar tabela se existir
        cursor.execute(query_drop_table)

        # Query de construção de tabela 
        query_create_table = f"""
-- Criar a tabela {table_name}
CREATE TABLE {table_name} AS
"""    
    else:
        # Quando satus=False, a função não cria a tabela
        query_drop_table=""
        query_create_table=""


    query_join=""
    lista_vars=""
    query_vars=""


    query_vars=f"""
{query_create_table}
WITH
tb_public as(
    SELECT
        {id},
        {safra_ref}
    FROM {tb_publico}
),
"""    
    for feat_cat_valor in lista_valor_agragador:

        for n in lista_janela:
            query_agragada, table_name, variaveis = snow_query_agregada(n, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra)
            query_vars+=f"""
            {query_agragada}
        """
            query_join +=f"""
    LEFT JOIN  {table_name}
            ON  tb_public.{id} = {table_name}.{id}
            AND tb_public.{safra_ref} = {table_name}.{safra_ref}
        """    
            lista_vars+=f"""
            {variaveis}
        """
    query_vars = query_vars.rstrip(', \n')

    lista_vars = lista_vars.rstrip(', \n')

    query_vars+=f"""

    SELECT 
        tb_public.{id},
        tb_public.{safra_ref},
        
        {lista_vars}
    FROM tb_public
    {query_join}

    """
    if status:
        # Executando query de criação da tabela
        cursor.execute(query_vars)
        cursor.close()
        
    # Salvando arquivo em file .sql:
    try: 
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(query_drop_table)
            arquivo.write(query_vars)
        print(f'Complete query creation with {nome_arquivo} saved file')
    except:
        print("Complete query creation with no saved file.")

    return query_vars
