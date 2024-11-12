from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, IntegerType

class Functions:
    @staticmethod
    def treat_flag_vars(col_name):
        # Quando o valor for positivo retorna 1 
        return F.when(F.col(col_name) == 'S', 1).otherwise(0)
    
    @staticmethod
    # Remove os espaços em branco ao fim de um string
    def trim_string(col_name):
        return F.trim(F.col(col_name))
    
class Constants:
    # Final Columns
    CODIGO_CLIENTE = "codigo_cliente"
    DATA_NASCIMENTO = "data_nascimento"
    IDADE = "idade"
    SEXO = "sexo"
    UF = "uf"
    CIDADE = "cidade"
    ESTADO_CIVIL = "estado_civil"
    FLAG_LGPD_CALL = "flag_lgpd_call"
    FLAG_LGPD_SMS = "flag_lgpd_sms"
    FLAG_LGPD_EMAIL = "flag_lgpd_email"
    FLAG_LGPD_PUSH = "flag_lgpd_push"

    # Columns from clientes raw table
    V_ID_CLI = "v_id_cli"
    D_DT_NASC = "d_dt_nasc"
    V_SX_CLI = "v_sx_cli"
    N_EST_CVL = "n_est_cvl"

    # Columns from clientes_opt raw table
    B_PUSH = "b_push"
    B_SMS = "b_sms"
    B_EMAIL = "b_email"
    B_CALL = "b_call"

    # Columns from enderecos_clientes raw table
    V_LCL = "v_lcl"
    V_UF = "v_uf"

class Variables:
    functions = Functions()

    # clientesColSeq - colunas da tabela "clientes_raw"
    clientes_col_seq = [
        F.col(Constants.V_ID_CLI).cast(StringType()).alias(Constants.CODIGO_CLIENTE),
        F.col(Constants.D_DT_NASC).cast(DateType()).alias(Constants.DATA_NASCIMENTO),
        F.col(Constants.V_SX_CLI).cast(StringType()).alias(Constants.SEXO),
        F.col(Constants.N_EST_CVL).cast(StringType()).alias(Constants.ESTADO_CIVIL)
    ]

    # clientesOptColSeq - colunas da tabela "clientes_opt_raw"
    clientes_opt_col_seq = [
        F.col(Constants.V_ID_CLI).cast(StringType()).alias(Constants.CODIGO_CLIENTE),
        F.col(Constants.B_PUSH).cast(StringType()).alias(Constants.FLAG_LGPD_CALL),
        F.col(Constants.B_SMS).cast(StringType()).alias(Constants.FLAG_LGPD_SMS),
        F.col(Constants.B_EMAIL).cast(StringType()).alias(Constants.FLAG_LGPD_EMAIL),
        F.col(Constants.B_CALL).cast(StringType()).alias(Constants.FLAG_LGPD_PUSH)
    ]

    # enderecosClientesColSeq - colunas da tabela "enderecos_clientes_raw"
    enderecos_clientes_col_seq = [
        F.col(Constants.V_ID_CLI).cast(StringType()).alias(Constants.CODIGO_CLIENTE),
        F.col(Constants.V_LCL).cast(StringType()).alias(Constants.CIDADE),
        F.col(Constants.V_UF).cast(StringType()).alias(Constants.UF)
    ]

    # flagVarsRules - regras de criação de variáveis de flags
    flag_vars_rules = [
        (Constants.FLAG_LGPD_CALL, functions.treat_flag_vars(Constants.FLAG_LGPD_CALL).cast(IntegerType())),
        (Constants.FLAG_LGPD_SMS, functions.treat_flag_vars(Constants.FLAG_LGPD_SMS).cast(IntegerType())),
        (Constants.FLAG_LGPD_EMAIL, functions.treat_flag_vars(Constants.FLAG_LGPD_EMAIL).cast(IntegerType())),
        (Constants.FLAG_LGPD_PUSH, functions.treat_flag_vars(Constants.FLAG_LGPD_PUSH).cast(IntegerType()))
    ]

    # endVarRules - regras de transformação para cidade e estado
    end_var_rules = [
        (Constants.CIDADE, functions.trim_string(Constants.CIDADE)),
        (Constants.UF, functions.trim_string(Constants.UF))
    ]

    # Funções para sequências de variáveis
    @staticmethod
    def minus2_flag_vars_seq():
        return [
            Constants.FLAG_LGPD_CALL,
            Constants.FLAG_LGPD_SMS,
            Constants.FLAG_LGPD_EMAIL,
            Constants.FLAG_LGPD_PUSH
        ]

    @staticmethod
    def minus2_cliente_seq():
        return [
            Constants.SEXO,
            Constants.ESTADO_CIVIL
        ]

    @staticmethod
    def minus2_endereco_seq():
        return [
            Constants.CIDADE,
            Constants.UF
        ]

    @staticmethod
    def string_col_seq():
        return [
            Constants.SEXO,
            Constants.UF,
            Constants.CIDADE,
            Constants.ESTADO_CIVIL
        ]

    @staticmethod
    def integer_col_seq():
        return [
            Constants.IDADE,
            Constants.FLAG_LGPD_CALL,
            Constants.FLAG_LGPD_SMS,
            Constants.FLAG_LGPD_EMAIL,
            Constants.FLAG_LGPD_PUSH
        ]
