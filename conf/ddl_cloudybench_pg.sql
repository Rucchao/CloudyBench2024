CREATE TABLE customer (
C_ID int PRIMARY KEY,
C_NAME char(15),
C_GENDER char(6),
C_AGE int,
C_PROVINCE char(15),
C_CITY char(15),
C_CREDIT real,
C_CREATEDDATE Date,
C_UPDATEDDATE timestamp
);

CREATE TABLE orders (
O_ID int PRIMARY KEY,
O_C_ID int,
O_DATE Date,
O_STATUS char(10),
O_TOTALAMOUNT real,
O_UPDATEDDATE timestamp
);

CREATE TABLE orderline (
OL_ID int PRIMARY KEY,
OL_O_ID int,
OL_P_ID int,
OL_QUANTITY int,
OL_TOTALAMOUNT real,
OL_UPDATEDDATE timestamp
);

