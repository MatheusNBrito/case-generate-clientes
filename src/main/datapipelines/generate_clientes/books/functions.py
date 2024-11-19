from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, when, datediff, floor, trim
from main.datapipelines.generate_clientes.books import constants
from typing import List 
from pyspark.sql import functions as F

class Functions:

    def trim_string(self, column_name: str):
        return trim(col(column_name))
    

    def create_idade_col(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            constants.IDADE,
            (F.floor(F.datediff(F.current_date(), F.col(constants.DATA_NASCIMENTO)) / 365)).cast("int")
        )

    def treat_flag_vars(self, column_name: str):
        return (
            when(col(column_name) == "false", 0)
            .otherwise(when(col(column_name) == "true", 1).otherwise(None))
        )

    def minus2_flag_vars_treatment(self, df: DataFrame) -> DataFrame:
        return df.na.fill(-2, self.minus2_flag_vars_seq())
    
    @staticmethod
    def minus2_string_treatment(minus2_seq: List[str], df: DataFrame) -> DataFrame:
        return df.na.fill("-2", minus2_seq)


    def minus1_treat_null(self, df: DataFrame) -> DataFrame:
        return (
            df.na.fill("-1", self.string_col_seq())
            .na.fill(-1, self.integer_col_seq())
        )

    def filter_null_rows(self, df: DataFrame) -> DataFrame:
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

    # Funções auxiliares
    def minus2_flag_vars_seq(self):
        return [
            constants.FLAG_LGPD_CALL,
            constants.FLAG_LGPD_SMS,
            constants.FLAG_LGPD_EMAIL,
            constants.FLAG_LGPD_PUSH
        ]

    def string_col_seq(self):
        return [
            constants.SEXO,
            constants.ESTADO_CIVIL,
            constants.CIDADE,
            constants.UF
        ]

    def integer_col_seq(self):
        return [
            constants.IDADE,
            constants.FLAG_LGPD_CALL,
            constants.FLAG_LGPD_SMS,
            constants.FLAG_LGPD_EMAIL,
            constants.FLAG_LGPD_PUSH
        ]
