from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from main.datapipelines.generate_clientes.books.functions import Functions
from main.datapipelines.generate_clientes.books.variables import Variables
from main.datapipelines.generate_clientes.books import constants

class Transformations:

    @staticmethod
    def generate_clientes(clientes_raw_df: DataFrame, clientes_opt_raw_df: DataFrame, enderecos_clientes_raw_df: DataFrame) -> DataFrame:
        # Aplicar transformações no DataFrame clientes_raw_df
        clientes_transformed_df = (
            clientes_raw_df
            .dropDuplicates()
            .filter(col("CODIGO_CLIENTE").isNotNull())
        )
        clientes_transformed_df = Functions.minus2_string_treatment(
            Variables.minus2_cliente_seq(),
            clientes_transformed_df
        )

        # Aplicar transformações no DataFrame clientes_opt_raw_df
        clientes_opt_transformed_df = (
            clientes_opt_raw_df
            .dropDuplicates()
            .filter(col("CODIGO_CLIENTE").isNotNull())
        )
        # Aplicar transformações nas flags
        for flag_column in ["FLAG_LGPD_CALL", "FLAG_LGPD_SMS", "FLAG_LGPD_EMAIL", "FLAG_LGPD_PUSH"]:
            clientes_opt_transformed_df = clientes_opt_transformed_df.withColumn(
                flag_column,
                Functions.treat_flag_vars(flag_column)  # Não é necessário `.cast(IntegerType())`, pois a função já retorna um número.
            )

        # Aplicar transformações no DataFrame enderecos_clientes_raw_df
        enderecos_clientes_transformed_df = (
            enderecos_clientes_raw_df
            .dropDuplicates()
            .filter(col("CODIGO_CLIENTE").isNotNull())
            .withColumn("CIDADE", Functions.trim_string("CIDADE"))
            .withColumn("UF", Functions.trim_string("UF"))
        )
        enderecos_clientes_transformed_df = Functions.minus2_string_treatment(
            Variables.minus2_endereco_seq(),
            enderecos_clientes_transformed_df
        )

        # Juntar todos os DataFrames
        final_clientes_df = (
            clientes_transformed_df
            .join(clientes_opt_transformed_df, ["CODIGO_CLIENTE"], "left")
            .join(enderecos_clientes_transformed_df, ["CODIGO_CLIENTE"], "left")
        )
        final_clientes_df = Functions.create_idade_col(final_clientes_df)
        final_clientes_df = Functions.minus1_treat_null(final_clientes_df)
        final_clientes_df = Functions.filter_null_rows(final_clientes_df)

        return final_clientes_df