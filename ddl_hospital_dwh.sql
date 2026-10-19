-- =====================================================================
-- DDL Star Schema: hospital_dwh  (MySQL 8 / MariaDB, engine InnoDB)
-- Bisa dijalankan manual di SQLyog, atau otomatis oleh etl_hospital.py
-- Menjalankan ulang file ini = MENGHAPUS dan membuat ulang semua tabel
-- =====================================================================
CREATE DATABASE IF NOT EXISTS hospital_dwh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE hospital_dwh;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fact_procedures;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_encounter_class;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS dim_payer;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS reject_log;
DROP TABLE IF EXISTS etl_cleansing_summary;

SET FOREIGN_KEY_CHECKS = 1;

-- ---------------------------------------------------------------- DIMENSI
CREATE TABLE dim_date (
    date_sk       INT          NOT NULL PRIMARY KEY,   -- format YYYYMMDD
    full_date     DATE         NOT NULL,
    year_num      SMALLINT     NOT NULL,
    quarter_num   TINYINT      NOT NULL,
    quarter_name  CHAR(2)      NOT NULL,
    month_num     TINYINT      NOT NULL,
    month_name    VARCHAR(10)  NOT NULL,
    day_of_month  TINYINT      NOT NULL,
    day_of_week   TINYINT      NOT NULL,               -- 1 = Senin ... 7 = Minggu
    day_name      VARCHAR(10)  NOT NULL,
    is_weekend    TINYINT      NOT NULL
) ENGINE = InnoDB;

CREATE TABLE dim_patient (
    patient_sk      INT          NOT NULL PRIMARY KEY,  -- -1 = Unknown member
    patient_bk      VARCHAR(36)  NOT NULL,              -- patients.Id (UUID)
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    full_name       VARCHAR(210),
    gender          CHAR(1),                            -- M / F / U
    birth_date      DATE,
    death_date      DATE,
    is_alive        TINYINT       NULL,
    race            VARCHAR(50),
    ethnicity       VARCHAR(50),
    marital_status  VARCHAR(30),
    city            VARCHAR(100),
    state           VARCHAR(60),
    zip             VARCHAR(15),
    lat             DECIMAL(9,6) NULL,                  -- koordinat untuk peta dashboard
    lon             DECIMAL(9,6) NULL,
    UNIQUE KEY uq_dim_patient_bk (patient_bk)
) ENGINE = InnoDB;

CREATE TABLE dim_payer (
    payer_sk        INT          NOT NULL PRIMARY KEY,  -- -1 = Unknown member
    payer_bk        VARCHAR(36)  NOT NULL,
    payer_name      VARCHAR(150),
    city            VARCHAR(100),
    state           VARCHAR(60),
    zip             VARCHAR(15),
    phone           VARCHAR(40),
    payer_category  VARCHAR(20),                        -- Public / Private / Self-Pay / Unknown
    UNIQUE KEY uq_dim_payer_bk (payer_bk)
) ENGINE = InnoDB;

CREATE TABLE dim_procedure (
    procedure_sk           INT           NOT NULL PRIMARY KEY,
    procedure_code         VARCHAR(40)   NOT NULL,
    procedure_description  VARCHAR(300),
    UNIQUE KEY uq_dim_procedure_code (procedure_code)
) ENGINE = InnoDB;

CREATE TABLE dim_encounter_class (
    encounter_class_sk    INT          NOT NULL PRIMARY KEY,   -- -1 = unknown
    encounter_class_name  VARCHAR(40)  NOT NULL,
    UNIQUE KEY uq_dim_encounter_class (encounter_class_name)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------- FAKTA
CREATE TABLE fact_encounters (
    encounter_sk                 BIGINT        NOT NULL PRIMARY KEY,
    encounter_bk                 VARCHAR(36)   NOT NULL,       -- degenerate dimension
    start_date_sk                INT           NOT NULL,
    patient_sk                   INT           NOT NULL,
    payer_sk                     INT           NOT NULL,
    encounter_class_sk           INT           NOT NULL,
    start_datetime               DATETIME      NOT NULL,
    stop_datetime                DATETIME      NOT NULL,
    base_encounter_cost          DECIMAL(14,2),
    total_claim_cost             DECIMAL(14,2),
    payer_coverage               DECIMAL(14,2),
    out_of_pocket_cost           DECIMAL(14,2),                -- total_claim_cost - payer_coverage
    encounter_duration_minutes   INT,
    is_readmission_30d           TINYINT       NOT NULL,       -- hanya untuk kelas inpatient
    UNIQUE KEY uq_fact_encounters_bk (encounter_bk),
    KEY ix_fe_date    (start_date_sk),
    KEY ix_fe_patient (patient_sk),
    KEY ix_fe_payer   (payer_sk),
    KEY ix_fe_class   (encounter_class_sk),
    CONSTRAINT fk_fe_date    FOREIGN KEY (start_date_sk)      REFERENCES dim_date (date_sk),
    CONSTRAINT fk_fe_patient FOREIGN KEY (patient_sk)         REFERENCES dim_patient (patient_sk),
    CONSTRAINT fk_fe_payer   FOREIGN KEY (payer_sk)           REFERENCES dim_payer (payer_sk),
    CONSTRAINT fk_fe_class   FOREIGN KEY (encounter_class_sk) REFERENCES dim_encounter_class (encounter_class_sk)
) ENGINE = InnoDB;

CREATE TABLE fact_procedures (
    procedure_fact_sk            BIGINT       NOT NULL PRIMARY KEY,
    encounter_sk                 BIGINT       NOT NULL,
    procedure_date_sk            INT          NOT NULL,
    patient_sk                   INT          NOT NULL,
    payer_sk                     INT          NOT NULL,
    procedure_sk                 INT          NOT NULL,
    start_datetime               DATETIME     NOT NULL,
    stop_datetime                DATETIME,
    base_cost                    DECIMAL(14,2),
    procedure_duration_minutes   INT,
    procedure_count              TINYINT      NOT NULL,        -- selalu 1, memudahkan SUM
    KEY ix_fp_encounter (encounter_sk),
    KEY ix_fp_date      (procedure_date_sk),
    KEY ix_fp_patient   (patient_sk),
    KEY ix_fp_payer     (payer_sk),
    KEY ix_fp_procedure (procedure_sk),
    CONSTRAINT fk_fp_encounter FOREIGN KEY (encounter_sk)      REFERENCES fact_encounters (encounter_sk),
    CONSTRAINT fk_fp_date      FOREIGN KEY (procedure_date_sk) REFERENCES dim_date (date_sk),
    CONSTRAINT fk_fp_patient   FOREIGN KEY (patient_sk)        REFERENCES dim_patient (patient_sk),
    CONSTRAINT fk_fp_payer     FOREIGN KEY (payer_sk)          REFERENCES dim_payer (payer_sk),
    CONSTRAINT fk_fp_procedure FOREIGN KEY (procedure_sk)      REFERENCES dim_procedure (procedure_sk)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------- LOG ETL
CREATE TABLE reject_log (
    reject_id     INT           NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source_table  VARCHAR(30)   NOT NULL,
    source_key    VARCHAR(200),
    rule_name     VARCHAR(100)  NOT NULL,
    reason        VARCHAR(255),
    raw_data      TEXT,
    rejected_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;

CREATE TABLE etl_cleansing_summary (
    summary_id    INT           NOT NULL AUTO_INCREMENT PRIMARY KEY,
    technique     VARCHAR(40)   NOT NULL,
    rule_name     VARCHAR(150)  NOT NULL,
    source_table  VARCHAR(30)   NOT NULL,
    rows_affected INT           NOT NULL,
    action        VARCHAR(255)
) ENGINE = InnoDB;