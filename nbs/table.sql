

WITH
tb_public as(
    SELECT
        ID,
        SAFRA_REF
    FROM tb_spine
),

            
-- Criação de variáveis agrupadas com janela de 1M
tb_agrupada_FEAT_CAT1_B_1M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 1
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_B_1M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_B_1M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_B_1M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_B_1M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_B_1M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 1
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_B_1M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_B_1M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_B_1M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_B_1M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_B_1M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 1 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'B'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
),

        
            
-- Criação de variáveis agrupadas com janela de 2M
tb_agrupada_FEAT_CAT1_B_2M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 2
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_B_2M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_B_2M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_B_2M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_B_2M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_B_2M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 2
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_B_2M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_B_2M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_B_2M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_B_2M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_B_2M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 2 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'B'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
),

        
            
-- Criação de variáveis agrupadas com janela de 3M
tb_agrupada_FEAT_CAT1_B_3M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 3
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_B_3M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_B_3M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_B_3M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_B_3M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_B_3M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 3
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_B_3M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_B_3M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_B_3M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_B_3M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_B_3M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 3 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'B'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
),

        
            
-- Criação de variáveis agrupadas com janela de 1M
tb_agrupada_FEAT_CAT1_C_1M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 1
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_C_1M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_C_1M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_C_1M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_C_1M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_C_1M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 1
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_C_1M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_C_1M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_C_1M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_C_1M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_C_1M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 1 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'C'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
),

        
            
-- Criação de variáveis agrupadas com janela de 2M
tb_agrupada_FEAT_CAT1_C_2M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 2
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_C_2M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_C_2M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_C_2M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_C_2M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_C_2M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 2
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_C_2M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_C_2M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_C_2M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_C_2M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_C_2M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 2 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'C'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
),

        
            
-- Criação de variáveis agrupadas com janela de 3M
tb_agrupada_FEAT_CAT1_C_3M as(
    SELECT
        tb_public.ID,
        tb_public.SAFRA_REF,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM1 para a janela 3
        SUM(COALESCE(tb_feat.FEAT_NUM1,0))  AS SUM_FEAT_NUM1_FEAT_CAT1_C_3M,
        MAX(COALESCE(tb_feat.FEAT_NUM1,0))  AS MAX_FEAT_NUM1_FEAT_CAT1_C_3M,
        MIN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MIN_FEAT_NUM1_FEAT_CAT1_C_3M,
        AVG(COALESCE(tb_feat.FEAT_NUM1,0))  AS AVG_FEAT_NUM1_FEAT_CAT1_C_3M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM1,0))  AS MEDIAN_FEAT_NUM1_FEAT_CAT1_C_3M,

        -- Criação de variáveis agrupadas a partir da coluna FEAT_CAT1 e FEAT_NUM2 para a janela 3
        SUM(COALESCE(tb_feat.FEAT_NUM2,0))  AS SUM_FEAT_NUM2_FEAT_CAT1_C_3M,
        MAX(COALESCE(tb_feat.FEAT_NUM2,0))  AS MAX_FEAT_NUM2_FEAT_CAT1_C_3M,
        MIN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MIN_FEAT_NUM2_FEAT_CAT1_C_3M,
        AVG(COALESCE(tb_feat.FEAT_NUM2,0))  AS AVG_FEAT_NUM2_FEAT_CAT1_C_3M,
        MEDIAN(COALESCE(tb_feat.FEAT_NUM2,0))  AS MEDIAN_FEAT_NUM2_FEAT_CAT1_C_3M
    FROM tb_public
    INNER JOIN tb_feat
        ON  tb_public.ID = tb_feat.ID
        AND (DATEADD('month', 3 , TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD')) >=  tb_public.SAFRA_REF) 
        AND (TO_DATE(tb_feat.SAFRA, 'YYYY-MM-DD') < tb_public.SAFRA_REF)
        AND tb_feat.FEAT_CAT1 = 'C'
    GROUP BY tb_public.ID, tb_public.SAFRA_REF
)

    SELECT 
        tb_public.ID,
        tb_public.SAFRA_REF,
        
        
            
        tb_agrupada_FEAT_CAT1_B_1M.SUM_FEAT_NUM1_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MAX_FEAT_NUM1_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MIN_FEAT_NUM1_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.AVG_FEAT_NUM1_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MEDIAN_FEAT_NUM1_FEAT_CAT1_B_1M,

        tb_agrupada_FEAT_CAT1_B_1M.SUM_FEAT_NUM2_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MAX_FEAT_NUM2_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MIN_FEAT_NUM2_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.AVG_FEAT_NUM2_FEAT_CAT1_B_1M,
        tb_agrupada_FEAT_CAT1_B_1M.MEDIAN_FEAT_NUM2_FEAT_CAT1_B_1M,

        
            
        tb_agrupada_FEAT_CAT1_B_2M.SUM_FEAT_NUM1_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MAX_FEAT_NUM1_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MIN_FEAT_NUM1_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.AVG_FEAT_NUM1_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MEDIAN_FEAT_NUM1_FEAT_CAT1_B_2M,

        tb_agrupada_FEAT_CAT1_B_2M.SUM_FEAT_NUM2_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MAX_FEAT_NUM2_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MIN_FEAT_NUM2_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.AVG_FEAT_NUM2_FEAT_CAT1_B_2M,
        tb_agrupada_FEAT_CAT1_B_2M.MEDIAN_FEAT_NUM2_FEAT_CAT1_B_2M,

        
            
        tb_agrupada_FEAT_CAT1_B_3M.SUM_FEAT_NUM1_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MAX_FEAT_NUM1_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MIN_FEAT_NUM1_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.AVG_FEAT_NUM1_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MEDIAN_FEAT_NUM1_FEAT_CAT1_B_3M,

        tb_agrupada_FEAT_CAT1_B_3M.SUM_FEAT_NUM2_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MAX_FEAT_NUM2_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MIN_FEAT_NUM2_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.AVG_FEAT_NUM2_FEAT_CAT1_B_3M,
        tb_agrupada_FEAT_CAT1_B_3M.MEDIAN_FEAT_NUM2_FEAT_CAT1_B_3M,

        
            
        tb_agrupada_FEAT_CAT1_C_1M.SUM_FEAT_NUM1_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MAX_FEAT_NUM1_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MIN_FEAT_NUM1_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.AVG_FEAT_NUM1_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MEDIAN_FEAT_NUM1_FEAT_CAT1_C_1M,

        tb_agrupada_FEAT_CAT1_C_1M.SUM_FEAT_NUM2_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MAX_FEAT_NUM2_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MIN_FEAT_NUM2_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.AVG_FEAT_NUM2_FEAT_CAT1_C_1M,
        tb_agrupada_FEAT_CAT1_C_1M.MEDIAN_FEAT_NUM2_FEAT_CAT1_C_1M,

        
            
        tb_agrupada_FEAT_CAT1_C_2M.SUM_FEAT_NUM1_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MAX_FEAT_NUM1_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MIN_FEAT_NUM1_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.AVG_FEAT_NUM1_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MEDIAN_FEAT_NUM1_FEAT_CAT1_C_2M,

        tb_agrupada_FEAT_CAT1_C_2M.SUM_FEAT_NUM2_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MAX_FEAT_NUM2_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MIN_FEAT_NUM2_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.AVG_FEAT_NUM2_FEAT_CAT1_C_2M,
        tb_agrupada_FEAT_CAT1_C_2M.MEDIAN_FEAT_NUM2_FEAT_CAT1_C_2M,

        
            
        tb_agrupada_FEAT_CAT1_C_3M.SUM_FEAT_NUM1_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MAX_FEAT_NUM1_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MIN_FEAT_NUM1_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.AVG_FEAT_NUM1_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MEDIAN_FEAT_NUM1_FEAT_CAT1_C_3M,

        tb_agrupada_FEAT_CAT1_C_3M.SUM_FEAT_NUM2_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MAX_FEAT_NUM2_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MIN_FEAT_NUM2_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.AVG_FEAT_NUM2_FEAT_CAT1_C_3M,
        tb_agrupada_FEAT_CAT1_C_3M.MEDIAN_FEAT_NUM2_FEAT_CAT1_C_3M
    FROM tb_public
    
    LEFT JOIN  tb_agrupada_FEAT_CAT1_B_1M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_B_1M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_B_1M.SAFRA_REF
        
    LEFT JOIN  tb_agrupada_FEAT_CAT1_B_2M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_B_2M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_B_2M.SAFRA_REF
        
    LEFT JOIN  tb_agrupada_FEAT_CAT1_B_3M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_B_3M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_B_3M.SAFRA_REF
        
    LEFT JOIN  tb_agrupada_FEAT_CAT1_C_1M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_C_1M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_C_1M.SAFRA_REF
        
    LEFT JOIN  tb_agrupada_FEAT_CAT1_C_2M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_C_2M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_C_2M.SAFRA_REF
        
    LEFT JOIN  tb_agrupada_FEAT_CAT1_C_3M
            ON  tb_public.ID = tb_agrupada_FEAT_CAT1_C_3M.ID
            AND tb_public.SAFRA_REF = tb_agrupada_FEAT_CAT1_C_3M.SAFRA_REF
        

    