# Removido por não haver uso dentro do repositório

# from pyspark.sql import functions as F
#
# from opin_lib_cap_ingestao_sro_dados.transformation.transformation_functions import normalize_multivalues_field, \
#     normalize_json_str, \
#     normalize_date_time_field, get_index_day_of_month, get_na_or_number, normalize_mask_monetary_values, \
#     normalize_mask_processo_susep_values, \
#     normalize_model
#
#
#
# normalize_multivalues_field_udf = F.udf(
#     lambda value, delimiter, default_value: normalize_multivalues_field(value, delimiter, default_value))
# get_value_or_default_udf = F.udf(lambda value, default_value: get_value_or_default(value, default_value))
# normalize_json_str_udf = F.udf(lambda value: normalize_json_str(value))
# normalize_date_time_field_udf = F.udf(lambda value, default_value: normalize_date_time_field(value, default_value))
# get_index_day_of_month_udf = F.udf(lambda value: get_index_day_of_month(value))
# get_na_or_number_udf = F.udf(lambda value, default_value: get_na_or_number(value, default_value))
# normalize_mask_monetary_values_udf = F.udf(lambda value, default_value: normalize_mask_monetary_values(value,
#                                                                                                        default_value))
# normalize_mask_processo_susep_values_udf = F.udf(
#     lambda value, default_value: normalize_mask_processo_susep_values(value,
#                                                                       default_value))
# normalize_model_udf = F.udf(lambda value, default_value: normalize_model(value, default_value))