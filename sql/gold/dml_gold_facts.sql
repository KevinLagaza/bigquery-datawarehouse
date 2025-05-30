-- =============================================================================
-- Création Table de Faits: gold.fact_sales
-- =============================================================================

CREATE OR REPLACE VIEW `<project_id>.gold.fact_sales` AS
select 
	csd.sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	csd.sls_order_dt as order_date,
	csd.sls_ship_dt as ship_date,
	csd.sls_due_dt as due_date,
	csd.sls_sales as sales_amount,
	csd.sls_quantity as quantity,
	csd.sls_price as price 

from `<project_id>.silver.crm_sales_details` csd

    left join `<project_id>.gold.dim_customers` dc 
        on csd.sls_cust_id = dc.customer_id

    left join `<project_id>.gold.dim_products` dp
        on csd.sls_prd_key = dp.product_number;