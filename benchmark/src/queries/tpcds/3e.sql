--q3.sql--

 SELECT dt.d_year, ss_ext_sales_price, ss_sold_date_sk
 FROM  date_dim dt, store_sales
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
   AND dt.d_moy=11
            
