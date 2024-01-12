CREATE SEQUENCE IF NOT EXISTS orders_id_seq;
SELECT SETVAL('orders_id_seq', (SELECT max(o_id) FROM orders));
ALTER TABLE orders ALTER COLUMN o_id SET DEFAULT nextval('orders_id_seq'::regclass);
ALTER sequence orders_id_seq owner to postgres;
ALTER SEQUENCE orders_id_seq OWNED BY orders.o_id;

CREATE SEQUENCE IF NOT EXISTS orderline_id_seq;
SELECT SETVAL('orderline_id_seq', (SELECT max(ol_id) FROM orderline));
ALTER TABLE orderline ALTER COLUMN ol_id SET DEFAULT nextval('orderline_id_seq'::regclass);
ALTER sequence orderline_id_seq owner to postgres;
ALTER SEQUENCE orderline_id_seq OWNED BY orderline.ol_id;