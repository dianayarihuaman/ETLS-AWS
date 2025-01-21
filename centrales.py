import awswrangler as wr
import pandas as pd
import boto3
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys

mandatory_params = [
    'TABLE_NAME',
    'CONFIG_KEY',
]

args = getResolvedOptions(sys.argv, mandatory_params)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(args["TABLE_NAME"])

#Lectura del elemento en la tabla de Dynamo:
config = table.get_item(
                Key={
                    'CEN': args["CONFIG_KEY"]
                }
            ).get('Item')

pd.set_option('display.max_columns', None)

def sendSNS(error):
    sns = boto3.client('sns')
    mandatory = []
    if "--ENV" in sys.argv and "--TOPIC" in sys.argv:
        mandatory.append('ENV')
        mandatory.append('TOPIC')
        print(mandatory)
        args = getResolvedOptions(sys.argv, mandatory)
        print(args)
        env = args['ENV']
        topic = args['TOPIC']
        print("env:",env)
        print("topic:",topic)

        FinalMessage = f"Instalaciones Centrales:  Ambiente: {env},  message: {error}"

        print("mensaje final", FinalMessage)

    
    sendError = sns.publish(
        TopicArn=topic,
        Message=FinalMessage
        )

def df_read(origin):
    df = wr.s3.read_parquet(path = origin)
    return df

def df_write(df,dest, element, write_mode, database ):
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

# Función que agrega columnas de año, mes y día a partir de una columna de fecha.
def add_partition_fields(df, formato = '%Y', col = "year"):
    df['year'] = [x for x in df[col]]
    return df

#### Lectura de los parametros del json de dynamodb
data        = config['params']
catalog_id  = config["catalog_id"]
data_params = json.loads(data) 

try:

    S3_origin_comunas            = data_params.get('S3_origin_comunas')
    S3_origin_Datos_Tecnicos     = data_params.get('S3_origin_Datos_Tecnicos')
    S3_origin_FT_Aplicada_Datos  = data_params.get('S3_origin_FT_Aplicada_Datos')
    S3_origin_FT_Aplicadas       = data_params.get('S3_origin_FT_Aplicadas')
    S3_origin_FT_Estandar_Datos  = data_params.get('S3_origin_FT_Estandar_Datos')
    S3_origin_Centrales          = data_params.get('S3_origin_Centrales')
    S3_origin_Centrales_Tipos    = data_params.get('S3_origin_Centrales_Tipos')
    S3_origin_Conceptos          = data_params.get('S3_origin_Conceptos')
    S3_origin_Empresas           = data_params.get('S3_origin_Empresas')
    S3_origin_Grupos             = data_params.get('S3_origin_Grupos')
    S3_origin_Instalaciones      = data_params.get('S3_origin_Instalaciones')
    ruta_destino                 = data_params.get('ruta_destino')
    table                        = data_params.get('table')
    write_mode                   = data_params.get('write_mode')
    database                     = data_params.get('database')
    info_conceptos               = data_params.get('info_conceptos')
except:
    print("Fallo la obtencion de datos de DYNAMO")
    sendSNS("Fallo la obtencion de datos de DYNAMO")

try:
    #Lectura de dataframes
    df_origin_comunas            = df_read(S3_origin_comunas)
    df_origin_Datos_Tecnicos     = df_read(S3_origin_Datos_Tecnicos)
    df_origin_FT_Aplicada_Datos  = df_read(S3_origin_FT_Aplicada_Datos)
    df_origin_FT_Aplicadas       = df_read(S3_origin_FT_Aplicadas)
    df_origin_FT_Estandar_Datos  = df_read(S3_origin_FT_Estandar_Datos)
    df_origin_Centrales          = df_read(S3_origin_Centrales)
    df_origin_Centrales_Tipos    = df_read(S3_origin_Centrales_Tipos)
    df_origin_Conceptos          = df_read(S3_origin_Conceptos)
    df_origin_Empresas           = df_read(S3_origin_Empresas)
    df_origin_Grupos             = df_read(S3_origin_Grupos)
    df_origin_Instalaciones      = df_read(S3_origin_Instalaciones)
except:
    print("Fallo la lectura de los dataframes")
    sendSNS("Fallo la lectura de los dataframes")

try:
    #Seleccionando columnas y renombrarlas
    df_origin_Centrales_select  = df_origin_Centrales[['id_central','centralnombre','centralnemotecnico','centraldescripcion','id_propietario','id_centrocontrol', 'id_coordinado' , 'id_comuna','id_centraltipo']]
    df_origin_Empresas_select          = df_origin_Empresas[['id_empresa','empresanombre']]
    df_origin_Empresas_select1         = df_origin_Empresas[['id_empresa','empresanombre']]
    df_origin_Grupos_select            = df_origin_Grupos[['id_grupo','gruponombre']]
    df_origin_comunas_select           = df_origin_comunas[['id_comuna','comunanombre']]
    df_origin_Centrales_Tipos_select   = df_origin_Centrales_Tipos[['id_centraltipo','centraltiponombre']]
    df_origin_FT_Aplicadas_select      = df_origin_FT_Aplicadas[['id_ftaplicada','id_instalacion']]
    df_origin_Instalaciones_select1    = df_origin_Instalaciones[['id_central','id_instalacion','id_instalaciontipo']]
    df_origin_FT_Estandar_Datos_select = df_origin_FT_Estandar_Datos[['id_fte_dato','id_datotecnico']]
    df_origin_Datos_Tecnicos_select    = df_origin_Datos_Tecnicos[['id_datotecnico','id_concepto']]
    df_origin_Conceptos_select         = df_origin_Conceptos[['id_concepto','conceptonombre']]

    # Renombrando columnas de empresa
    columns_propietario                = {'id_empresa':'id_empresa_propietario', 'empresanombre':'propietario'}
    df_origin_propietario1             = df_origin_Empresas_select1.rename(columns = columns_propietario, inplace = False  )
    df_origin_propietario_select       = df_origin_propietario1[['id_empresa_propietario','propietario']]
    columns_centraltipo                = {'id_centraltipo':'id_centraltipo_2'}
    df_origin_Centrales_Tipos_rename   = df_origin_Centrales_Tipos_select.rename(columns = columns_centraltipo, inplace = False  )
    columns_aplicadas                  = {'id_ftaplicada':'id_ftaplicada2', 'id_instalacion':'id_instalacion2'}
    df_origin_FT_Aplicadas_rename      = df_origin_FT_Aplicadas_select.rename(columns = columns_aplicadas, inplace = False  )
    columns_instalacion                = {'id_central':'id_central2', 'id_instalacion':'id_instalacion3'}
    df_origin_Instalaciones_rename     = df_origin_Instalaciones_select1.rename(columns = columns_instalacion, inplace = False  )
    columns_estandar                   = {'id_fte_dato':'id_fte_dato2'}
    df_origin_FT_Estandar_Datos_rename = df_origin_FT_Estandar_Datos_select.rename(columns = columns_estandar, inplace = False  )
    columns_tecnicos                   = {'id_datotecnico':'id_datotecnico2', 'id_concepto':'id_concepto2'}
    df_origin_Datos_Tecnicos_rename    = df_origin_Datos_Tecnicos_select.rename(columns = columns_tecnicos, inplace = False  )

    # Aplicando filtros
    df_origin_Instalaciones_select     = df_origin_Instalaciones_rename[df_origin_Instalaciones_rename['id_instalaciontipo']==3]
    df_instalaciones_unidades          = df_origin_Instalaciones_rename[df_origin_Instalaciones_rename['id_instalaciontipo']==4]

    # JOINs correspondientes para obtener las centrales
    df1 = df_origin_Centrales_select.join(df_origin_Empresas_select.set_index('id_empresa'), on ='id_centrocontrol' , how='left')
    df2 = df1.join(df_origin_propietario_select.set_index('id_empresa_propietario'), on ='id_propietario' , how='left')
    df3 = df2.join(df_origin_Grupos_select.set_index('id_grupo'), on ='id_coordinado' , how='left')
    df4 = df3.join(df_origin_comunas_select.set_index('id_comuna'), on ='id_comuna' , how='left')
    df5 = df4.join(df_origin_Centrales_Tipos_rename.set_index('id_centraltipo_2'), on ='id_centraltipo' , how='left')
    df6 = df5[['id_central','centralnombre', 'empresanombre', 'propietario', 'gruponombre', 'comunanombre', 'centraltiponombre', 'centralnemotecnico', 'centraldescripcion']]
    df_centrales = df6

    # JOINs correspondientes para obtener las centrales
    df7  = df_origin_Centrales_select.join(df_origin_Instalaciones_select.set_index('id_central2'), on ='id_central' , how='inner')
    df8  = df7.join(df_origin_FT_Aplicadas_rename.set_index('id_instalacion2'), on ='id_instalacion3' , how='inner')
    df9  = df8.join(df_origin_FT_Aplicada_Datos.set_index('id_ftaplicada'), on ='id_ftaplicada2' , how='inner')
    df10 = df9.join(df_origin_FT_Estandar_Datos_rename.set_index('id_fte_dato2'), on ='id_fte_dato' , how='inner') 
    df11 = df10.join(df_origin_Datos_Tecnicos_rename.set_index('id_datotecnico2'), on ='id_datotecnico' , how='inner') 
    df12 = df11.join(df_origin_Conceptos_select.set_index('id_concepto'), on ='id_concepto2' , how='inner') 
    df13 = df12[['centralnombre','id_concepto2','conceptonombre','fta_datovalortexto']]

    # Aplicando filtros
    df_conceptos  = df13[df13['id_concepto2'].isin([311, 312, 313, 314, 803, 844, 1418, 1580, 1635, 783, 845, 846, 847, 848, 849, 850, 851, 1460, 1629, 1630, 1631, 1632, 1633, 1790, 1791, 1804, 1805, 2897, 744])]

    # Obteniendo conteo de unidades generadoras por central
    dfu1              = df_origin_Centrales_select.join(df_instalaciones_unidades.set_index('id_central2'), on ='id_central' , how='left')
    dfu1_select       = dfu1[['centralnombre']]
    dfu1_group_2      = dfu1_select.groupby(['centralnombre']).agg(cantidad_unidades=pd.NamedAgg(column="centralnombre", aggfunc="count"))
    df_conceptos_todo = df_centrales.merge(dfu1_group_2, left_on='centralnombre', right_index=True)

    # Pivoteando columnas
    df_conceptos = df_conceptos.drop_duplicates(subset=['centralnombre', 'conceptonombre'])
    df_conceptos1 = df_conceptos.pivot( index='centralnombre' , columns="conceptonombre", values="fta_datovalortexto")
    df_resultado_pivot = df_conceptos_todo.merge(df_conceptos1 , left_on='centralnombre', right_index=True)
    df_resultado_pivot.loc[:, ~df_resultado_pivot.columns.isin(['CentralNombre','ID_Concepto2','ConceptoNombre','FTA_DatoValorTexto'])]

    df_resultado_pivot  = df_resultado_pivot[['id_central','centralnombre','empresanombre','propietario','gruponombre','comunanombre','centraltiponombre','centralnemotecnico','centraldescripcion','Estado (operativa/en pruebas/en construcción)','cantidad_unidades','11.1.2 Puntos de conexión al SI a través de los cuales inyecta energía.','11.1.4 Potencia máxima bruta, para cada tipo de combustible que pueda operar','11.1.5 Consumos propios como % de la potencia máxima bruta','11.1.6 Capacidad máxima, potencia neta efectiva','11.1.7 Potencia mínima técnica, para cada combustible que pueda operar','11.1.11 Fecha de entrada en operación','Distribuidora','Diagrama PQ equivalente de la central','10.1.35 Tipo de conversión de energía','10.1.35 Convencional / ERNC','11.1.35 Combustible (solo para termoeléctricas)','10.1.35 Medio de generación según DS 125-2019 y DS 88-2020','11.1.38 Tipo de tecnología de la central','11.1.38 Región','11.1.8 Provincia','11.1.8 Comuna','11.1.38 Número de comunas de emplazamiento','11.1.38 Sala de máquinas: comuna','11.1.38 Bocatoma: comuna','11.1.38 Represa: comuna','11.1.38 Embalse comuna 1','11.1.38 Embalse comuna 2','11.1.38 Embalse comuna 3','11.1.39 Coordenada este','11.1.39 Coordenada norte','11.1.39 Zona o huso [ej: 18H, 19J]','11.1.40 Parámetros de partida y detención','1 Diagrama unilineal, señalando capacidad nominal de equipos primarios  (*.dwg y *.pdf)']]

    # Escribiendo en Zone Analytics 
    df_write(df_resultado_pivot,ruta_destino, table, write_mode, database )

except:
    print("Fallaron los cruces de datos")