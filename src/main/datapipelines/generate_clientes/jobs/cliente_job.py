from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import configparser
from main.datapipelines.generate_clientes.commons.session.spark_session import SparkSessionWrapper  
from main.datapipelines.generate_clientes.books.variables import Variables
from main.datapipelines.generate_clientes.books.functions import Functions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col

class ClienteJob:
    def __init__(self):
        # Inicializa a sessão Spark através do wrapper
        self.spark_session_wrapper = SparkSessionWrapper()
        self.spark = self.spark_session_wrapper.spark
        print(self.spark)

    # Função para carregar os dados
    def load_data(self, raw_tables):
        clientes_raw_df = self.spark.read.parquet(raw_tables["CLIENTES_PATH"]).select(*Variables.clientes_col_seq)
        clientes_opt_raw_df = self.spark.read.json(raw_tables["CLIENTES_OPT_PATH"]).select(*Variables.clientes_opt_col_seq)
        enderecos_clientes_raw_df = self.spark.read.parquet(raw_tables["ENDERECOS_CLIENTES_PATH"]).select(*Variables.enderecos_clientes_col_seq)
        clientes_raw_df.printSchema()
        clientes_opt_raw_df.printSchema()
        enderecos_clientes_raw_df.printSchema()
        return clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df
        

    # Função para salvar os dados
    def save_data(self, df, output_paths):
        df.write.mode("overwrite").parquet(output_paths)

    # Função para aplicar as transformações
    def generate_clientes(self, clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df):

        # Tratamento de clientes_raw_df
        print("Iniciando transformação de clientes_raw_df")
        clientes_transformed_df = clientes_raw_df.dropDuplicates().filter(col("CODIGO_CLIENTE").isNotNull())
        string_columns_clientes = [col_name for col_name in clientes_transformed_df.columns if col_name != "data_nascimento"]
        clientes_transformed_df = Functions.minus2_string_treatment(string_columns_clientes, clientes_transformed_df)
        clientes_transformed_df.show()

        # Tratamento de clientes_opt_raw_df
        print("Iniciando transformação de clientes_opt_raw_df")
        clientes_opt_transformed_df = clientes_opt_raw_df.dropDuplicates().filter(col("CODIGO_CLIENTE").isNotNull())
        clientes_opt_transformed_df = Functions.minus2_string_treatment(clientes_opt_transformed_df.columns, clientes_opt_transformed_df)
        clientes_opt_transformed_df.show()

        # Tratamento de enderecos_clientes_raw_df
        print("Iniciando transformação de enderecos_clientes_raw_df")
        enderecos_clientes_transformed_df = enderecos_clientes_raw_df.dropDuplicates().filter(col("CODIGO_CLIENTE").isNotNull())
        enderecos_clientes_transformed_df = Functions.minus2_string_treatment(enderecos_clientes_transformed_df.columns, enderecos_clientes_transformed_df)
        enderecos_clientes_transformed_df.show()

        # União dos DataFrames tratados
        final_clientes_df = clientes_transformed_df.join(clientes_opt_transformed_df, on="CODIGO_CLIENTE", how="left") \
                                                   .join(enderecos_clientes_transformed_df, on="CODIGO_CLIENTE", how="left")
        return final_clientes_df

    # Função principal
    def run_job(self):
        try:
            # Lê o arquivo de configuração
            conf = configparser.ConfigParser()
            conf.read('C:/Projetos/panvel-generate-clientes/src/main/datapipelines/generate_clientes/resources/aplication.conf')

            # Acessa as configurações
            raw_tables = {
                "CLIENTES_PATH": conf["input_paths"]["raw_tables.CLIENTES_PATH"],
                "CLIENTES_OPT_PATH": conf["input_paths"]["raw_tables.CLIENTES_OPT_PATH"],
                "ENDERECOS_CLIENTES_PATH": conf["input_paths"]["raw_tables.ENDERECOS_CLIENTES_PATH"]
            }
            output_path_clientes = conf["output_paths"]["CLIENTES_PATH"]
            print(f"CLIENTES_PATH: {raw_tables['CLIENTES_PATH']}")

            # Carrega os dados
            clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df = self.load_data(raw_tables)

            # Aplica as transformações
            final_clientes_df = self.generate_clientes(clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df)

            # Salva os dados
            self.save_data(final_clientes_df, output_path_clientes)

        except Exception as e:
            print(f"Erro durante a execução do job: {e}")
            raise  # Propaga o erro para análise

    def stop(self):
        # Fecha a sessão Spark
        if self.spark:
            self.spark.stop()


if __name__ == "__main__":
    job = ClienteJob()
    try:
        job.run_job()
    finally:
        job.stop()