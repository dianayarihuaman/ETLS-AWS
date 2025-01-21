import awswrangler as wr
import pandas as pd
import boto3
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys
import utils as utils
mandatory_params = [
    'TABLE_NAME',
    'CONFIG_KEY',
]
args = getResolvedOptions(sys.argv, mandatory_params)
#Instanciando para interacción con DYNAMO
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(args["TABLE_NAME"])
#Lectura del elemento en la tabla de Dynamo:
config = table.get_item(
    Key={
        'CEN': args["CONFIG_KEY"]
    }
).get('Item')
pd.set_option("display.max_rows", None, "display.max_columns", None)
def write_count_dynamo(df,S3,table,database):
    Count = len(df.index)
    print(Count)
    ambiente = database.split("_")
    stage =  ambiente[1]
    ERROR = None  # Inicialización del error.
    STATUS = True
    
    if Count < 1:
        ERROR = "Failed: bucket empty"
        STATUS = False
                                                          
    utils.save_log(database,STATUS,ERROR,Count,S3,table,stage)

def df_write(df,dest, element, write_mode, database,catalog_id ):
    wr.s3.to_parquet(
                df=df, 
                path=dest, 
                index=False,
                dataset=True,
                compression = 'gzip',
                mode = write_mode,
                database=database,
                table=element,
                catalog_id = catalog_id,
                schema_evolution = True,
                sanitize_columns = False)

def dim_empresas_combustibles(df_coordinated, S3_dest_dim_empresas_combustibles,S3_dest_dim_empresas_combustibles_staging, config):
    #### Obteniendo campos del select
    df_coordinated_res1   = df_coordinated[['id','name','register_status']]
    df_coordinated_res    = df_coordinated_res1[df_coordinated["register_status"] == 'A']
    
    #### Cambiando nombre a columnas 
    columns_co = {'id':'id_empresa','name':'nombre_empresa'}
    df_coordinated_rename = df_coordinated_res.rename(columns = columns_co, inplace = False)
    df_coordinated_select = df_coordinated_rename[['id_empresa','nombre_empresa']]
    df_coordinated_res    = df_coordinated_select.drop_duplicates()
    datatypes = {'id_empresa': 'int32'}
    df_empresas = df_coordinated_res.astype(datatypes)
    write_count_dynamo(df_empresas,S3_dest_dim_empresas_combustibles,"dim_scvic_empresas",config["database"])
    
    ### Escribimos el dataframe resultado en S3_dest_dim_centrales
    df_write(df_empresas,S3_dest_dim_empresas_combustibles,"dim_scvic_empresas","overwrite_partitions",config["database"],config["catalog_id"])
    df_write(df_empresas,S3_dest_dim_empresas_combustibles_staging,"dim_scvic_empresas","overwrite_partitions",config["database_staging"],config["catalog_id"])

def dim_centrales_combustibles(df_power_stations,df_gas_pipelines,df_gnl_terminals, S3_dest_dim_centrales_combustibles,S3_dest_dim_centrales_combustibles_staging, config):
    
    #### Cambiando nombre a columnas 
    df_power_stations_select = df_power_stations[['id','name']]
    df_gas_pipelines_select  = df_gas_pipelines[['id','name']]
    df_gnl_terminals_select  = df_gnl_terminals[['id','name']]
    
    df_centrales_res1 = pd.concat([df_power_stations_select,df_gas_pipelines_select, df_gnl_terminals_select], ignore_index=True )
    df_centrales_res2 = df_centrales_res1[['id','name']]
    
    columns_ce = {'id':'id_central', 'name':'nombre_central'}
    df_centrales_res2_rename = df_centrales_res2.rename(columns = columns_ce, inplace = False)
    df_centrales_select = df_centrales_res2_rename[['id_central','nombre_central']]
    df_centrales_res    = df_centrales_select.drop_duplicates()
    datatypes = {'id_central': 'int32'}
    df_centrales = df_centrales_res.astype(datatypes)
    
    write_count_dynamo(df_centrales,S3_dest_dim_centrales_combustibles,"dim_scvic_centrales",config["database"])
    
    ### Escribimos el dataframe resultado en S3_dest_dim_centrales
    df_write(df_centrales,S3_dest_dim_centrales_combustibles,"dim_scvic_centrales","overwrite_partitions",config["database"],config["catalog_id"])
    df_write(df_centrales,S3_dest_dim_centrales_combustibles_staging,"dim_scvic_centrales","overwrite_partitions",config["database_staging"],config["catalog_id"])

def dim_tipos_combustibles(df_fuels_types , S3_dest_dim_tipos_combustibles,S3_dest_dim_tipos_combustibles_staging, config):
    
    #### Obteniendo campos para realizar los JOIN
    df_fuels_types_dic               = df_fuels_types[['id','name']]
    
    #### Cambiando nombre a columnas 
    columns_ft = {'id':'id_tipo_combustible','name':'nombre_tipo_combustible'}
    df_tipo_combustible_rename = df_fuels_types_dic.rename(columns = columns_ft, inplace = False)
    df_tipo_combustible_select = df_tipo_combustible_rename[['id_tipo_combustible','nombre_tipo_combustible']]
    df_tipo_combustible_res = df_tipo_combustible_select.drop_duplicates()
    
    datatypes = {'id_tipo_combustible': 'int32'}
    df_dim_tipos_combustibles = df_tipo_combustible_res.astype(datatypes)
    write_count_dynamo(df_dim_tipos_combustibles,S3_dest_dim_tipos_combustibles,"dim_scvic_combustibles",config["database"])
    
    ### Escribimos el dataframe resultado en S3_dest_dim_centrales
    df_write(df_dim_tipos_combustibles,S3_dest_dim_tipos_combustibles,"dim_scvic_combustibles","overwrite_partitions",config["database"],config["catalog_id"])
    df_write(df_dim_tipos_combustibles,S3_dest_dim_tipos_combustibles_staging,"dim_scvic_combustibles","overwrite_partitions",config["database_staging"],config["catalog_id"])

def dim_tipos_stock(df_declarations_stocks_types, df_declarations_groups,df_titles_stocks_reports , S3_dest_dim_tipos_stock,S3_dest_dim_tipos_stock_staging, config):
    #### Obteniendo campos SELECT   
    df_declarations_stocks_types_select  = df_declarations_stocks_types[['id','declarations_groups_type_id','titles_stocks_report_id']]
    df_declarations_groups_select        = df_declarations_groups[['id','name']]
    df_titles_stocks_reports_select      = df_titles_stocks_reports[['id','name']]
    
    #### Cambiando nombre a columnas 
    columns_ds = {'id':'id_declarations_stocks_types'}
    columns_dg = {'id':'id_declarations_groups'   ,'name':'nombre_grupo'}
    columns_ts = {'id':'id_titles_stocks_reports' ,'name':'nombre_titulo_grupo'}
    
    df_declarations_stocks_types_rename = df_declarations_stocks_types_select.rename(columns = columns_ds, inplace = False)
    df_declarations_groups_rename       = df_declarations_groups_select.rename(columns = columns_dg, inplace = False)
    df_titles_stocks_reports_rename     = df_titles_stocks_reports_select.rename(columns = columns_ts, inplace = False)
    
    df_declarations_stocks_types_select = df_declarations_stocks_types_rename[['id_declarations_stocks_types','declarations_groups_type_id','titles_stocks_report_id']]
    df_declarations_groups_select       = df_declarations_groups_rename[['id_declarations_groups','nombre_grupo']]
    df_titles_stocks_reports_select     = df_titles_stocks_reports_rename[['id_titles_stocks_reports','nombre_titulo_grupo']]
    ####  JOin entre tablas
    df_tipos_stock_1 = df_declarations_stocks_types_select.join(df_declarations_groups_select.set_index('id_declarations_groups'), on='declarations_groups_type_id', how='inner')
    df_tipos_stock_2 = df_tipos_stock_1.join(df_titles_stocks_reports_select.set_index('id_titles_stocks_reports'), on='titles_stocks_report_id', how='inner')
    df_tipos_stock_3 = df_tipos_stock_2[['id_declarations_stocks_types','nombre_grupo','titles_stocks_report_id','nombre_titulo_grupo']]
    
    #### Cambiando nombre a columnas 
    columns_ts = {'titles_stocks_report_id':'id_titles_stocks_report' }
    df_tipos_stock_4 = df_tipos_stock_3.rename(columns = columns_ts, inplace = False)
    df_tipos_stock_5 = df_tipos_stock_4[['id_declarations_stocks_types','nombre_grupo','id_titles_stocks_report','nombre_titulo_grupo']]
    datatypes = {'id_titles_stocks_report': 'string'}
    df_dim_tipos_stock = df_tipos_stock_5.astype(datatypes)
    
    write_count_dynamo(df_dim_tipos_stock,S3_dest_dim_tipos_stock,"dim_scvic_tipos_stock",config["database"])
    
    ### Escribimos el dataframe resultado en S3_dest_dim_centrales
    df_write(df_dim_tipos_stock,S3_dest_dim_tipos_stock,"dim_scvic_tipos_stock","overwrite_partitions",config["database"],config["catalog_id"])
    df_write(df_dim_tipos_stock,S3_dest_dim_tipos_stock_staging,"dim_scvic_tipos_stock","overwrite_partitions",config["database_staging"],config["catalog_id"])

def dim_Unidad_Generadora(df_configurations, df_power_plant,S3_dest_dim_Unidad_Generadora,S3_dest_dim_Unidad_Generadora_staging, config):
    #### Cambiando nombre a columnas 
    columns_ft = {'id':'id_configuracion_unidad','name':'nombre_configuracion_unidad'}
    df_configurations_rename = df_configurations.rename(columns = columns_ft, inplace = False)
    df_configurations_select = df_configurations_rename[['id_configuracion_unidad','nombre_configuracion_unidad','power_plant_id']]
    
    ####  JOin entre tablas
    df_unidad_generadora_res1 = df_configurations_select.join(df_power_plant.set_index('id'), on='power_plant_id', how='inner') 
    df_unidad_generadora_res2 = df_unidad_generadora_res1[['id_configuracion_unidad','nombre_configuracion_unidad','power_plant_id','name']]
    
    #### Cambiando nombre a columnas resultado
    columns_ug = {'power_plant_id':'id_unidad_generadora','name':'nombre_unidad_generadora'}
    df_unidad_generadora_res3 = df_unidad_generadora_res2.rename(columns = columns_ug, inplace = False)
    df_unidad_generadora_res4 = df_unidad_generadora_res3[['id_configuracion_unidad','id_unidad_generadora','nombre_unidad_generadora','nombre_configuracion_unidad']]
    
    datatypes = {'id_configuracion_unidad': 'int32','id_unidad_generadora': 'int32' }
    df_unidad_generadora_res = df_unidad_generadora_res4.astype(datatypes)
    write_count_dynamo(df_unidad_generadora_res,S3_dest_dim_Unidad_Generadora,"dim_scvic_unidad_generadora",config["database"])
    
    ### Escribimos el dataframe resultado en S3_dest_dim_centrales
    df_write(df_unidad_generadora_res,S3_dest_dim_Unidad_Generadora,"dim_scvic_unidad_generadora","overwrite_partitions",config["database"],config["catalog_id"])
    df_write(df_unidad_generadora_res,S3_dest_dim_Unidad_Generadora_staging,"dim_scvic_unidad_generadora","overwrite_partitions",config["database_staging"],config["catalog_id"])

    
#### Lectura de los parametros del json de dynamodb
data = config['params']
data_params = json.loads(data) 
#### Lectura fuentes en dataframe
df_coordinated               = wr.s3.read_parquet(path=data_params["S3_origin_coordinated"])
df_power_stations            = wr.s3.read_parquet(path=data_params["S3_origin_power_stations"])
df_configurations            = wr.s3.read_parquet(path=data_params["S3_origin_configurations"])
df_power_plant               = wr.s3.read_parquet(path=data_params["S3_origin_power_plant"])
df_declarations_stocks       = wr.s3.read_parquet(path=data_params["S3_origin_declarations_stocks"])
df_declarations_stocks_types = wr.s3.read_parquet(path=data_params["S3_origin_declarations_stocks_types"])
df_supplies_fuels_groups     = wr.s3.read_parquet(path=data_params["S3_origin_supplies_fuels_groups"])
df_declarations_groups       = wr.s3.read_parquet(path=data_params["S3_origin_declarations_groups"])
df_titles_stocks_reports     = wr.s3.read_parquet(path=data_params["S3_origin_titles_stocks_reports"])
df_fuels_types               = wr.s3.read_parquet(path=data_params["S3_origin_fuels_types"])
df_gas_pipelines             = wr.s3.read_parquet(path=data_params["S3_origin_gas_pipelines"])
df_gnl_terminals             = wr.s3.read_parquet(path=data_params["S3_origin_gnl_terminals"])
#### Creacion de dimensiones
dim_empresas_combustibles(df_coordinated, data_params["S3_dest_dim_empresas_combustibles"],data_params["S3_dest_dim_empresas_combustibles_staging"], config)
dim_centrales_combustibles(df_power_stations,df_gas_pipelines,df_gnl_terminals, data_params["S3_dest_dim_centrales_combustibles"],data_params["S3_dest_dim_centrales_combustibles_staging"], config)
dim_tipos_combustibles(df_fuels_types , data_params["S3_dest_dim_tipos_combustibles"],data_params["S3_dest_dim_tipos_combustibles_staging"], config)
dim_tipos_stock(df_declarations_stocks_types, df_declarations_groups,df_titles_stocks_reports , data_params["S3_dest_dim_tipos_stock"],data_params["S3_dest_dim_tipos_stock_staging"], config)
dim_Unidad_Generadora(df_configurations, df_power_plant,data_params["S3_dest_dim_Unidad_Generadora"],data_params["S3_dest_dim_Unidad_Generadora_staging"], config)