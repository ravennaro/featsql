# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/02_creation_mysql.ipynb.

# %% auto 0
__all__ = ['mysql_query_janela_num', 'mysql_query_join_num', 'mysql_create_query_num', 'mysql_query_janela_cat',
           'mysql_create_query_cat', 'mysql_query_agregada', 'mysql_query_join_agregada', 'mysql_create_query_agregada']

# %% ../nbs/02_creation_mysql.ipynb 4
def mysql_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra):
    query_janela = ""
    lista_vars_join = ""
    lista_vars_janelas = ""

    for n in lista_janela:
        query_variaveis_numericas = ""
        for i in feat_num_lista:
            query_variaveis_numericas += f"""
             -- Criação de variáveis numéricas a partir da coluna {i} para a janela {n}
            SUM(IFNULL({tb_feat}.{i},0)) AS {i}_SUM_{n}M,
            MIN(IFNULL({tb_feat}.{i},0)) AS {i}_MIN_{n}M,
            MAX(IFNULL({tb_feat}.{i},0)) AS {i}_MAX_{n}M,
            AVG(IFNULL({tb_feat}.{i},0)) AS {i}_AVG_{n}M,
            """
            lista_vars_join += f"""
            tb_join.{i}_SUM_{n}M,
            tb_join.{i}_MIN_{n}M,
            tb_join.{i}_MAX_{n}M,
            tb_join.{i}_AVG_{n}M,
            """
            lista_vars_janelas += f"""
            tb_janela_{n}M.{i}_SUM_{n}M,
            tb_janela_{n}M.{i}_MIN_{n}M,
            tb_janela_{n}M.{i}_MAX_{n}M,
            tb_janela_{n}M.{i}_AVG_{n}M,
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
            AND DATE_ADD({tb_feat}.{safra}, INTERVAL {n} MONTH) >= tb_public.{safra_ref}
            AND {tb_feat}.{safra} < tb_public.{safra_ref}
            GROUP BY tb_public.{id}, tb_public.{safra_ref}
        ),
        """
    
    lista_vars_join = lista_vars_join.rstrip(', \n')
    lista_vars_janelas = lista_vars_janelas.rstrip(', \n')
    return query_janela, lista_vars_join, lista_vars_janelas


# %% ../nbs/02_creation_mysql.ipynb 5
def mysql_query_join_num(lista_janela, id, safra_ref):
    query_join = ""
    for i in lista_janela:
        query_join += f"""
        LEFT JOIN tb_janela_{i}M
            ON tb_public.{id} = tb_janela_{i}M.{id}
            AND tb_public.{safra_ref} = tb_janela_{i}M.{safra_ref}
    """
    return query_join

# %% ../nbs/02_creation_mysql.ipynb 6
def mysql_create_query_num(tb_publico, # public table: contains the public, target and reference date
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

    query_janela, lista_vars_join, lista_vars_janelas = mysql_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra)
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
        {mysql_query_join_num(lista_janela, id, safra_ref)}
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

# %% ../nbs/02_creation_mysql.ipynb 8
def mysql_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra):
    vars_cat = ""
    query_janela_cat = ""
    join_moda = ""
    for n in lista_janela:
        for i in feat_cat_lista:
            query_janela_cat += f"""
    tb_janela_{i}_{n}M AS (
        SELECT
            tb_public.{id},
            tb_public.{safra_ref},
            {tb_feat}.{i},
            COUNT(*) AS frequency_{i}
        FROM tb_public
        LEFT JOIN {tb_feat}
        ON tb_public.{id} = {tb_feat}.{id}
            AND DATE_ADD({tb_feat}.{safra}, INTERVAL {n} MONTH) >= tb_public.{safra_ref}
            AND {tb_feat}.{safra} < tb_public.{safra_ref}
        GROUP BY tb_public.{id}, tb_public.{safra_ref}, {tb_feat}.{i}
    ),

    tb_row_{i}_{n}M AS (
        SELECT 
            *,    
            ROW_NUMBER() OVER (
                PARTITION BY 
                    {id},
                    {safra_ref}        
                    ORDER BY frequency_{i} DESC
            ) AS row_num_{i}_{n}M
        FROM tb_janela_{i}_{n}M
    ),
    
    tb_moda_{i}_{n}M AS (
        SELECT
            tb_row_{i}_{n}M.{id},
            tb_row_{i}_{n}M.{safra_ref},
            tb_row_{i}_{n}M.{i} AS {i}_MODA_{n}M
        FROM tb_row_{i}_{n}M 
        WHERE row_num_{i}_{n}M = 1
    ),
"""
            vars_cat += f"""
        tb_moda_{i}_{n}M.{i}_MODA_{n}M,
                 """
            join_moda += f"""
    LEFT JOIN tb_moda_{i}_{n}M 
    ON tb_moda_{i}_{n}M.{id} = tb_public.{id}
    AND tb_moda_{i}_{n}M.{safra_ref} = tb_public.{safra_ref}
"""
    vars_cat = vars_cat.rstrip(', \n')
    query_janela_cat = query_janela_cat.rstrip(', \n')
    return query_janela_cat, vars_cat, join_moda

# %% ../nbs/02_creation_mysql.ipynb 9
def mysql_create_query_cat(tb_publico, # public table: contains the public, target and reference date
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

    query_janela_cat, lista_vars, join_moda  = mysql_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra)

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
    {join_moda}
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

# %% ../nbs/02_creation_mysql.ipynb 11
def mysql_query_agregada(janela, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra):  
    lista_vars=""
    lista_vars_janelas=""  
    query= f"""
    tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M as(
        SELECT
            tb_public.{id},
            tb_public.{safra_ref},
"""
    for feat_num in lista_feat_num:
        query +=f"""
            SUM(COALESCE({tb_feat}.{feat_num},0))  AS SUM_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            MAX(COALESCE({tb_feat}.{feat_num},0))  AS MAX_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            MIN(COALESCE({tb_feat}.{feat_num},0))  AS MIN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            AVG(COALESCE({tb_feat}.{feat_num},0))  AS AVG_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
"""     
        lista_vars += f"""
            tb_join.SUM_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_join.MAX_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_join.MIN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_join.AVG_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
"""
        lista_vars_janelas += f"""
            tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.SUM_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.MAX_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.MIN_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
            tb_agrupada_{feat_cat}_{feat_cat_valor}_{janela}M.AVG_{feat_num}_{feat_cat}_{feat_cat_valor}_{janela}M,
"""
    query = query.rstrip(', \n')
    query +=f"""
        FROM tb_public
            INNER JOIN {tb_feat} 
            ON  tb_public.{id} = {tb_feat}.{id}
            AND DATE_ADD({tb_feat}.{safra}, INTERVAL {janela} MONTH) >= tb_public.{safra_ref}
            AND {tb_feat}.{safra} < tb_public.{safra_ref}
            AND {tb_feat}.{feat_cat} = '{feat_cat_valor}'
        GROUP BY tb_public.{id}, tb_public.{safra_ref} 
),
"""
    return query, lista_vars, lista_vars_janelas

# %% ../nbs/02_creation_mysql.ipynb 12
def mysql_query_join_agregada(janelas, feat_cat, lista_valor_agragador, id, safra_ref):
    query_join= ""
    for feat_cat_valor in lista_valor_agragador:
        for i in janelas:
            query_join += f"""
        LEFT JOIN tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M
            ON tb_public.{id} = tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M.{id}
            AND tb_public.{safra_ref} = tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M.{safra_ref}
        """
    return query_join

# %% ../nbs/02_creation_mysql.ipynb 13
def mysql_create_query_agregada(tb_publico, # public table: contains the public, target and reference date
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


    query=f"""
        {query_create_table}
        WITH
        tb_public as(
        SELECT
            {id},
            {safra_ref}
        FROM {tb_publico}
        ),
    """    
    lista_vars_janelas=""
    lista_vars_janelas_join=""
    for feat_cat_valor in lista_valor_agragador:
        for n in lista_janela:
            query_agragada, lista_vars_jan_join, lista_vars_jan = mysql_query_agregada(n, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra)
            query+=f"""
            {query_agragada}
        """
            lista_vars_janelas_join += lista_vars_jan_join
            lista_vars_janelas += lista_vars_jan
    lista_vars_janelas_join = lista_vars_janelas_join.rstrip(', \n')            
    lista_vars_janelas = lista_vars_janelas.rstrip(', \n')
    query +=f""" 
    tb_join AS (
        SELECT 
            tb_public.{id},
            tb_public.{safra_ref},
                    {lista_vars_janelas}   
        FROM tb_public 
                {mysql_query_join_agregada(lista_janela, feat_cat, lista_valor_agragador, id, safra_ref)}
            )

        SELECT 
            tb_join.{id},
            tb_join.{safra_ref},
            {lista_vars_janelas_join}
        FROM tb_join
    """

    if status:
        # Executando query de criação da tabela
        cursor.execute(query)
        cursor.close()
        
    # Salvando arquivo em file .sql:
    try: 
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(query_drop_table)
            arquivo.write(query)
        print(f'Complete query creation with {nome_arquivo} saved file')
    except:
        print("Complete query creation with no saved file.")
    return query
