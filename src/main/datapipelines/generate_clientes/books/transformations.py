from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from datapipelines.generate_clientes.books import constants, functions, variables

class Transformations:

    @staticmethod
    def generate_clientes(clientes_raw_df: DataFrame, clientes_opt_raw_df: DataFrame, enderecos_clientes_raw_df: DataFrame) -> DataFrame:
        # Aplicar transformações no DataFrame clientes_raw_df
        clientes_transformed_df = (
            clientes_raw_df
            .dropDuplicates()
            .filter(col(constants.CODIGO_CLIENTE).isNotNull())
            .transform(functions.minus2_string_treatment(variables.minus2_cliente_seq()))
        )

        # Aplicar transformações no DataFrame clientes_opt_raw_df
        clientes_opt_transformed_df = (
            clientes_opt_raw_df
            .dropDuplicates()
            .filter(col(constants.CODIGO_CLIENTE).isNotNull())
            .withColumn(constants.FLAG_LGPD_CALL, functions.treat_flag_vars(constants.FLAG_LGPD_CALL).cast(IntegerType()))
            .withColumn(constants.FLAG_LGPD_SMS, functions.treat_flag_vars(constants.FLAG_LGPD_SMS).cast(IntegerType()))
            .withColumn(constants.FLAG_LGPD_EMAIL, functions.treat_flag_vars(constants.FLAG_LGPD_EMAIL).cast(IntegerType()))
            .withColumn(constants.FLAG_LGPD_PUSH, functions.treat_flag_vars(constants.FLAG_LGPD_PUSH).cast(IntegerType()))
            .transform(functions.minus2_flag_vars_treatment)
        )

        # Aplicar transformações no DataFrame enderecos_clientes_raw_df
        enderecos_clientes_transformed_df = (
            enderecos_clientes_raw_df
            .dropDuplicates()
            .filter(col(constants.CODIGO_CLIENTE).isNotNull())
            .withColumn(constants.CIDADE, functions.trim_string(constants.CIDADE))
            .withColumn(constants.UF, functions.trim_string(constants.UF))
            .transform(functions.minus2_string_treatment(variables.minus2_endereco_seq()))
        )

        # Juntar todos os DataFrames
        final_clientes_df = (
            clientes_transformed_df
            .join(clientes_opt_transformed_df, [constants.CODIGO_CLIENTE], "left")
            .join(enderecos_clientes_transformed_df, [constants.CODIGO_CLIENTE], "left")
            .transform(functions.create_idade_col)
            .transform(functions.minus1_treat_null)
            .transform(functions.filter_null_rows)
        )

        return final_clientes_df
