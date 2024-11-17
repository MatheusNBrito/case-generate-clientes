from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, IntegerType
from main.datapipelines.generate_clientes.books import constants
from pyspark.sql import SparkSession

# Inicialização do SparkSession
spark = SparkSession.builder.appName("GenerateClientesJob").getOrCreate()

class Functions:
    @staticmethod
    def treat_flag_vars(col_name):
        # Quando o valor for positivo retorna 1 
        return F.when(F.col(col_name) == 'S', 1).otherwise(0)
    
    @staticmethod
    def trim_string(col_name):
        # Remove os espaços em branco ao fim de um string
        return F.trim(F.col(col_name))
    
class Variables:
    # Referência para funções auxiliares
    functions = Functions

    # clientesColSeq - colunas da tabela "clientes_raw"
    clientes_col_seq = [
        F.col(constants.V_ID_CLI).cast(StringType()).alias(constants.CODIGO_CLIENTE),
        F.col(constants.D_DT_NASC).cast(DateType()).alias(constants.DATA_NASCIMENTO),
        F.col(constants.V_SX_CLI).cast(StringType()).alias(constants.SEXO),
        F.col(constants.N_EST_CVL).cast(StringType()).alias(constants.ESTADO_CIVIL)
    ]

    # clientesOptColSeq - colunas da tabela "clientes_opt_raw"
    clientes_opt_col_seq = [
        F.col(constants.V_ID_CLI).cast(StringType()).alias(constants.CODIGO_CLIENTE),
        F.col(constants.B_PUSH).cast(StringType()).alias(constants.FLAG_LGPD_CALL),
        F.col(constants.B_SMS).cast(StringType()).alias(constants.FLAG_LGPD_SMS),
        F.col(constants.B_EMAIL).cast(StringType()).alias(constants.FLAG_LGPD_EMAIL),
        F.col(constants.B_CALL).cast(StringType()).alias(constants.FLAG_LGPD_PUSH)
    ]

    # enderecosClientesColSeq - colunas da tabela "enderecos_clientes_raw"
    enderecos_clientes_col_seq = [
        F.col(constants.V_ID_CLI).cast(StringType()).alias(constants.CODIGO_CLIENTE),
        F.col(constants.V_LCL).cast(StringType()).alias(constants.CIDADE),
        F.col(constants.V_UF).cast(StringType()).alias(constants.UF)
    ]

    # flagVarsRules - regras de criação de variáveis de flags
    flag_vars_rules = [
        (constants.FLAG_LGPD_CALL, Functions.treat_flag_vars(constants.FLAG_LGPD_CALL).cast(IntegerType())),
        (constants.FLAG_LGPD_SMS, Functions.treat_flag_vars(constants.FLAG_LGPD_SMS).cast(IntegerType())),
        (constants.FLAG_LGPD_EMAIL, Functions.treat_flag_vars(constants.FLAG_LGPD_EMAIL).cast(IntegerType())),
        (constants.FLAG_LGPD_PUSH, Functions.treat_flag_vars(constants.FLAG_LGPD_PUSH).cast(IntegerType()))
    ]

    # endVarRules - regras de transformação para cidade e estado
    end_var_rules = [
        (constants.CIDADE, Functions.trim_string(constants.CIDADE)),
        (constants.UF, Functions.trim_string(constants.UF))
    ]

    # Funções para sequências de variáveis
    @staticmethod
    def minus2_flag_vars_seq():
        return [
            constants.FLAG_LGPD_CALL,
            constants.FLAG_LGPD_SMS,
            constants.FLAG_LGPD_EMAIL,
            constants.FLAG_LGPD_PUSH
        ]

    @staticmethod
    def minus2_cliente_seq():
        return [
            constants.SEXO,
            constants.ESTADO_CIVIL
        ]

    @staticmethod
    def minus2_endereco_seq():
        return [
            constants.CIDADE,
            constants.UF
        ]

    @staticmethod
    def string_col_seq():
        return [
            constants.SEXO,
            constants.UF,
            constants.CIDADE,
            constants.ESTADO_CIVIL
        ]

    @staticmethod
    def integer_col_seq():
        return [
            constants.IDADE,
            constants.FLAG_LGPD_CALL,
            constants.FLAG_LGPD_SMS,
            constants.FLAG_LGPD_EMAIL,
            constants.FLAG_LGPD_PUSH
        ]
