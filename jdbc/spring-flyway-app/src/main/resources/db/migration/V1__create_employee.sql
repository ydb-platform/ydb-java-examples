CREATE TABLE employee
(
    id                  Int64 NOT NULL,
    full_name           Text,
    email               Text,
    hire_date           Date,
    salary              Decimal(22, 9),
    is_active           Bool,
    department          Text,
    age                 Int32,
    limit_date_password Datetime,

    PRIMARY KEY (id)
)