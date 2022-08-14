from datetime import datetime

today = datetime.now().strftime("%Y-%m-%d")

# Create Schema
CREATE_SCHEMA = """
    DROP SCHEMA IF EXISTS anime CASCADE;
    DROP SCHEMA IF EXISTS anime_stage CASCADE;
    CREATE SCHEMA IF NOT EXISTS anime;
    CREATE SCHEMA IF NOT EXISTS anime_stage;
"""
CREATE_SCHEMA_STUB = """
    DROP SCHEMA IF EXISTS %s CASCADE;
    CREATE SCHEMA IF NOT EXISTS %s;
"""
# Dim Day
CREATE_DIM_DAY = """
    CREATE TABLE IF NOT EXISTS public.dim_day (
        day_key SERIAL,
        _date DATE,
        day_of_month INT,
        month_num INT,
        quarter INT,
        _year INT,
        month_name TEXT,
        day_of_week TEXT,
        day_of_year INT,
        week_of_year INT,
        day_of_quarter INT,
        month_start_day TEXT,
        month_end_day TEXT,
        last_day_of_month INT,
        total_days_in_year INT,
        PRIMARY KEY (day_key)
    );

    TRUNCATE TABLE public.dim_day RESTART IDENTITY;

    -- Insert NULL row
    INSERT INTO public.dim_day(
        _date, day_of_month, month_num, quarter, 
        _year, month_name, day_of_week, day_of_year, 
        week_of_year, day_of_quarter, 
        month_start_day, month_end_day, last_day_of_month, 
        total_days_in_year
    )
    VALUES ('1000-12-31', NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL
    );

    -- Insert Dim_Day
    INSERT INTO public.dim_day(
        _date, day_of_month, month_num, quarter, _year, 
        month_name, day_of_week, day_of_year, week_of_year, 
        day_of_quarter, month_start_day, 
        month_end_day, last_day_of_month, 
        total_days_in_year)
    WITH dim_day AS (
    SELECT
        ROW_NUMBER() OVER
        (ORDER BY
            series.day)
        AS date_key
        ,series.day::DATE
        AS _date
        ,EXTRACT(DAY FROM series.day::DATE)
        AS day_of_month
        ,EXTRACT(MONTH FROM series.day::DATE)
        AS month_num
        ,EXTRACT(QUARTER FROM series.day::DATE)
        AS quarter
        ,EXTRACT(YEAR FROM series.day::DATE)
        AS _year
        ,TO_CHAR(series.day::DATE,'Month')
        AS month_name
        ,TO_CHAR(series.day::DATE,'Day')
        AS day_of_week
        ,EXTRACT(DOY FROM series.day::DATE)
        AS day_of_year
        ,EXTRACT(WEEK FROM series.day::DATE)
        AS week_of_year
    FROM
        GENERATE_SERIES
            ('1970-01-01'::DATE
            ,'2050-01-01'::DATE,'1 Day')
        AS series(day)
    )
    SELECT
        _date
        ,day_of_month
        ,month_num
        ,quarter
        ,_year
        ,month_name
        ,day_of_week
        ,day_of_year
        ,week_of_year
        ,ROW_NUMBER() OVER
        (PARTITION BY
            quarter
            ,_year
        ORDER BY
            _date
        )
        AS day_of_quarter
        ,TO_CHAR((MAKE_DATE(_year::INT,month_num::INT,1))::DATE
                ,'Day')
        AS month_start_day
        ,TO_CHAR((MAKE_DATE(_year::INT,month_num::INT,1)
                + '1 Month'::INTERVAL 
                - '1 Day'::INTERVAL)::DATE
                ,'Day')
        AS month_end_day
        ,EXTRACT(DAY FROM 
                (MAKE_DATE(_year::INT,month_num::INT,1)
                + '1 Month'::INTERVAL 
                - '1 Day'::INTERVAL)::DATE)
        AS last_day_of_month
        ,EXTRACT(DOY FROM MAKE_DATE(_year::INT,12,31))
        AS total_days_in_year
    FROM
        dim_day
    ORDER BY
        date_key;
        
    CREATE INDEX IF NOT EXISTS idx_date ON public.dim_day(_date);
"""
ENABLE_CROSSTAB = "CREATE EXTENSION IF NOT EXISTS tablefunc;"
