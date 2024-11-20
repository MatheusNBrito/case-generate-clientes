from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when,trim
from main.datapipelines.generate_clientes.books import constants
from typing import List 
from pyspark.sql import functions as F

class Functions:

    @staticmethod
    def trim_string(column_name: str):
         return trim(col(column_name))
    

    @staticmethod
    def create_idade_col(df: DataFrame) -> DataFrame:
        return df.withColumn(
            constants.IDADE,
            (F.floor(F.datediff(F.current_date(), F.col(constants.DATA_NASCIMENTO)) / 365)).cast("int")
    )

    @staticmethod
    def treat_flag_vars(column_name: str):
        return (
            when(col(column_name) == "false", 0)
            .otherwise(when(col(column_name) == "true", 1).otherwise(None))
        )

    @staticmethod
    def minus2_flag_vars_seq() -> list:
        # Retorne aqui a lista de colunas para o tratamento de valores "-2".
        return ["FLAG_LGPD_CALL", "FLAG_LGPD_SMS", "FLAG_LGPD_EMAIL", "FLAG_LGPD_PUSH"]

    @staticmethod
    def minus2_flag_vars_treatment(df: DataFrame) -> DataFrame:
        return df.na.fill(-2, Functions.minus2_flag_vars_seq())
    
    @staticmethod
    def minus2_string_treatment(minus2_seq: List[str], df: DataFrame) -> DataFrame:
        return df.na.fill("-2", minus2_seq)


    @staticmethod
    def minus1_treat_null(df: DataFrame) -> DataFrame:
        return (
            df.na.fill("-1", Functions.string_col_seq())  # Use a classe para acessar o método estático
            .na.fill(-1, Functions.integer_col_seq())     # O mesmo para o segundo método
    )


    @staticmethod
    def filter_null_rows(df: DataFrame) -> DataFrame:
        return df.filter(
            ~(
                (col(constants.DATA_NASCIMENTO).isNull()) &
                (col(constants.SEXO).isin("-1", "-2", None)) &
                (col(constants.ESTADO_CIVIL).isin("-1", "-2", None)) &
                (col(constants.FLAG_LGPD_CALL).isin(-1, -2, None)) &
                (col(constants.FLAG_LGPD_SMS).isin(-1, -2, None)) &
                (col(constants.FLAG_LGPD_EMAIL).isin(-1, -2, None)) &
                (col(constants.FLAG_LGPD_PUSH).isin(-1, -2, None)) &
                (col(constants.CIDADE).isin("-1", "-2", None)) &
                (col(constants.UF).isin("-1", "-2", None)) &
                (col(constants.IDADE).isin(-1, -2, None))
            )
        )

    @staticmethod
    def string_col_seq() -> list:
        return [
            constants.SEXO,
            constants.ESTADO_CIVIL,
            constants.CIDADE,
            constants.UF,
    ]

    @staticmethod
    def integer_col_seq() -> list:
        return [
            constants.IDADE,
            constants.FLAG_LGPD_CALL,
            constants.FLAG_LGPD_SMS,
            constants.FLAG_LGPD_EMAIL,
            constants.FLAG_LGPD_PUSH,
    ]

