# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_creation_sqlite.ipynb.

# %% auto 0
__all__ = ['sqlite_query_janela_num', 'sqlite_query_join_num', 'sqlite_create_query_num', 'sqlite_query_janela_cat',
           'sqlite_create_query_cat', 'sqlite_query_agregada', 'sqlite_query_join_agregada',
           'sqlite_create_query_agregada']

# %% ../nbs/01_creation_sqlite.ipynb 4
def sqlite_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra):
    query_janela = ""
    lista_vars = ""

    for n in lista_janela:
        query_variaveis_numericas=""
        for i in feat_num_lista:
            query_variaveis_numericas +=f"""
             -- Criação de variáveis numéricas a partir da coluna {i} para a janela {n}
            SUM(COALESCE({tb_feat}.{i},0)) AS {i}_SUM_{n}M,
            MIN(COALESCE({tb_feat}.{i},0)) AS {i}_MIN_{n}M,
            MAX(COALESCE({tb_feat}.{i},0)) AS {i}_MAX_{n}M,
            AVG(COALESCE({tb_feat}.{i},0)) AS {i}_AVG_{n}M,
            """
            lista_vars += f"""
            tb_join.{i}_SUM_{n}M,
            tb_join.{i}_MIN_{n}M,
            tb_join.{i}_MAX_{n}M,
            tb_join.{i}_AVG_{n}M,
            """
        query_variaveis_numericas = query_variaveis_numericas.rstrip(', \n')

        query_janela += f"""
        -- Criação de variáveis de janela de {n}M
        tb_janela_{n}M as(
            SELECT 
                tb_public.{id},
                tb_public.{safra_ref},
                {query_variaveis_numericas}

            FROM tb_public
            INNER JOIN {tb_feat} 
            ON  tb_public.{id} = tb_feat.{id}
            AND (strftime('%Y-%m-%d', date({tb_feat}.{safra}, '+{n} months')) >= tb_public.{safra_ref})
                AND ({tb_feat}.{safra} < tb_public.{safra_ref})
            GROUP BY tb_public.{id}, tb_public.{safra_ref}
        ),
        """
    lista_vars = lista_vars.rstrip(', \n')
    return query_janela, lista_vars

# %% ../nbs/01_creation_sqlite.ipynb 5
def sqlite_query_join_num(lista_janela, id, safra_ref):
    query_join= ""
    for i in lista_janela:
        query_join += f"""
        LEFT JOIN tb_janela_{i}M
            ON tb_public.{id} = tb_janela_{i}M.{id}
            AND tb_public.{safra_ref} = tb_janela_{i}M.{safra_ref}
    """
    return query_join

# %% ../nbs/01_creation_sqlite.ipynb 6
def sqlite_create_query_num(tb_publico, tb_feat, lista_janela,feat_num_lista, id, safra_ref, safra):
    
    query_janela, lista_var = sqlite_query_janela_num(lista_janela, feat_num_lista, id, safra_ref, tb_feat, safra)
    query_num = f"""
    WITH 
    tb_public AS (
        SELECT 
            *
        FROM {tb_publico}
    ),
    {query_janela}

    tb_join AS (
        SELECT 
            *
        FROM tb_public 
        {sqlite_query_join_num(lista_janela, id, safra_ref)}
    )
        
    SELECT 
        tb_join.{id},
        tb_join.{safra_ref},
        {lista_var}
    FROM tb_join
    """
    return query_num

# %% ../nbs/01_creation_sqlite.ipynb 8
def sqlite_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra):
  
    vars_cat = ""
    query_janela_cat = ""
    join_moda = ""
    for n in lista_janela:
        for i in feat_cat_lista:
            query_janela_cat += f"""
    tb_janela_{i}_{n}M as(
        SELECT
            tb_public.{id},
            tb_public.{safra_ref},
            {tb_feat}.{i},
            COUNT(*) AS frequency_{i}
        FROM tb_public
        LEFT JOIN {tb_feat}
        ON tb_public.{id} = {tb_feat}.{id}
            AND (strftime('%Y-%m-%d', date({tb_feat}.{safra}, '+{n} months')) >= tb_public.{safra_ref})
            AND ({tb_feat}.{safra} < tb_public.{safra_ref})
        GROUP BY tb_public.{id}, tb_public.{safra_ref}, {tb_feat}.{i}
    ),

    tb_row_{i}_{n}M as (
        SELECT 
            *,    
            ROW_NUMBER() OVER (
                PARTITION BY 
                    {id},
                    {safra_ref}        
                    ORDER BY frequency_{i} DESC
            ) as row_num_{i}_{n}M
        FROM tb_janela_{i}_{n}M
    ),
    
    tb_moda_{i}_{n}M AS(
        SELECT
            tb_row_{i}_{n}M .{id},
            tb_row_{i}_{n}M .{safra_ref},
            tb_row_{i}_{n}M.{i} AS {i}_MODA_{n}M
        FROM tb_row_{i}_{n}M 
        WHERE row_num_{i}_{n}M = 1
    ),
"""
            vars_cat +=f"""
        tb_moda_{i}_{n}M.{i}_MODA_{n}M,
                 """
            join_moda +=f"""
    LEFT JOIN tb_moda_{i}_{n}M 
    ON tb_moda_{i}_{n}M.{id} = tb_public.{id}
    AND tb_moda_{i}_{n}M.{safra_ref} = tb_public.{safra_ref}
"""
    vars_cat = vars_cat.rstrip(', \n')
    query_janela_cat = query_janela_cat.rstrip(', \n')
    return query_janela_cat, vars_cat, join_moda

# %% ../nbs/01_creation_sqlite.ipynb 9
def sqlite_create_query_cat(tb_publico, tb_feat, lista_janela,feat_cat_lista, id, safra_ref, safra):
    query_janela_cat, lista_vars, join_moda  = sqlite_query_janela_cat(lista_janela, feat_cat_lista, id, safra_ref, tb_feat, safra)

    query_num_cat = f"""
    WITH 
    tb_public as (
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
    return query_num_cat

# %% ../nbs/01_creation_sqlite.ipynb 11
def sqlite_query_agregada(janela, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra):  
    lista_vars= ""  
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
    query = query.rstrip(', \n')
    query +=f"""
        FROM tb_public
        INNER JOIN {tb_feat}
            ON tb_public.{id} = {tb_feat}.{id}
            AND (strftime('%Y-%m-%d', date({tb_feat}.{safra}, '+{janela} months')) >= tb_public.{safra_ref})
            AND ({tb_feat}.{safra} < tb_public.{safra_ref})
            AND {tb_feat}.{feat_cat} = '{feat_cat_valor}'
        GROUP BY tb_public.{id}, tb_public.{safra_ref} 
),
"""
    return query, lista_vars

# %% ../nbs/01_creation_sqlite.ipynb 12
def sqlite_query_join_agregada(janelas, feat_cat, lista_valor_agragador, id, safra_ref):
    query_join= ""
    for feat_cat_valor in lista_valor_agragador:
        for i in janelas:
            query_join += f"""
            LEFT JOIN tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M
                ON tb_public.{id} = tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M.{id}
                AND tb_public.{safra_ref} = tb_agrupada_{feat_cat}_{feat_cat_valor}_{i}M.{safra_ref}
        """
    return query_join

# %% ../nbs/01_creation_sqlite.ipynb 13
def sqlite_create_query_agregada(tb_publico, tb_feat, lista_janela, lista_feat_num, id, safra_ref, safra, feat_cat, lista_valor_agragador):
    
    query=f"""
        WITH
        tb_public as(
        SELECT
            {id},
            {safra_ref}
        FROM {tb_publico}
        ),
    """    
    lista_vars_janelas=""

    for feat_cat_valor in lista_valor_agragador:
        for n in lista_janela:
            query_agragada, lista_vars_jan = sqlite_query_agregada(n, lista_feat_num, feat_cat, feat_cat_valor, id, safra_ref, tb_feat, safra)
            query+=f"""
            {query_agragada}
        """
            lista_vars_janelas += lista_vars_jan
    lista_vars_janelas = lista_vars_janelas.rstrip(', \n')
    query +=f""" 
            tb_join AS (
                SELECT 
                    *        
                FROM tb_public 
                {sqlite_query_join_agregada(lista_janela, feat_cat, lista_valor_agragador, id, safra_ref)}
            )

        SELECT 
            tb_join.{id},
            tb_join.{safra_ref},
            {lista_vars_janelas}
        FROM tb_join
    """
    return query
