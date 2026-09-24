-- Preparação do ambiente para o pipeline Olist.
CREATE CATALOG IF NOT EXISTS olist_mvp;

CREATE SCHEMA IF NOT EXISTS olist_mvp.bronze;
CREATE SCHEMA IF NOT EXISTS olist_mvp.silver;
CREATE SCHEMA IF NOT EXISTS olist_mvp.gold;

-- Volume para upload dos oito arquivos CSV de origem.
CREATE VOLUME IF NOT EXISTS olist_mvp.bronze.raw_files;