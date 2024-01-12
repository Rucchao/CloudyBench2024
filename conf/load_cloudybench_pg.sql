\copy customer from 'Data_SF1/sales_customer.csv' CSV DELIMITER ',' ;

\copy orders from 'Data_SF1/sales_order.csv' CSV DELIMITER ',' ;

\copy orderline from 'Data_SF1/sales_orderline.csv' CSV DELIMITER ',' ;