with source as (
    select * from {{ ref('itens_pedido') }}
),

transformado as (
    select
        -- Chaves
        id_item_pedido,
        id_pedido,
        
        -- Detalhes do produto
        produto,
        categoria,
        quantidade,
        valor_unitario,
        valor_total_item,
        
        -- Metadados
        current_timestamp as etl_inserted_at
        
    from source
)

select * from transformado
