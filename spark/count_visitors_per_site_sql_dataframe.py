#!/usr/bin/env python

import sys
import codecs

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession\
    .builder\
    .appName("Basic SQL/DataFrame example on LTW data") \
    .getOrCreate()

# FIXME: To make sure we output non-ASCII to stdout in PySpark. Valid for Python 2.x.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# This could be provided as a parameter from an external configuration file.
fields = [
    StructField('NomSite', StringType()),
    StructField('IdVisiteur', StringType()),
    StructField('IdVisit', IntegerType()),
    StructField('debutDateVisit', TimestampType()),
    StructField('finDateVisit', TimestampType()),
    StructField('TotalTimeVisit', IntegerType()),
    StructField('Origine', StringType()),
    StructField('location_city', StringType()),
    StructField('location_country', StringType()),
    StructField('location_region', StringType()),
    StructField('referer_keyword', StringType()),
    StructField('referer_name', StringType()),
    StructField('referer_type', IntegerType()),
    StructField('referer_url', StringType()),
    StructField('IdAction', StringType()),
    StructField('DateAction', TimestampType()),
    StructField('URL', StringType()),
    StructField('NomAction', StringType()),
    StructField('TimeAction', IntegerType()),
    StructField('identreprise', IntegerType()),
    StructField('adresse6', StringType()),
    StructField('deptasp', IntegerType()),
    StructField('siege', IntegerType()),
    StructField('siteweb', StringType()),
    StructField('DCRENASP', StringType()),
    StructField('DCRETASP', StringType()),
    StructField('apen700', StringType()),
    StructField('APET700', StringType()),
    StructField('tefen', IntegerType()),
    StructField('tefet', IntegerType()),
    StructField('cj', IntegerType()),
    StructField('ca', IntegerType())
]

csv_configuration = {
    'sep': '\t',
    'schema': StructType(fields),
    'timestampFormat': 'dd/MM/yyyy HH:mm:ss'
}


def main(inputs):
    df = spark.read.csv(inputs, **csv_configuration)
    # FIXME: Leave commented when running on the cluster, may provoke encoding errors.
    # df.show()

    keep_fields = ['NomSite', 'IdVisiteur', 'debutDateVisit']
    df_light = df.drop(*[col for col in df.columns if col not in keep_fields])

    # DataFrame query.
    df_light \
        .select('NomSite', 'IdVisiteur')\
        .groupBy('NomSite') \
        .agg(countDistinct('IdVisiteur').alias('num_visits'))\
        .orderBy('num_visits', ascending=False)\
        .show()

    # SQL query.
    df_light.createOrReplaceTempView("my_view")
    spark.sql("""
        SELECT NomSite,
          COUNT(DISTINCT(IdVisiteur)) AS num_visits
        FROM my_view
        GROUP BY NomSite
        ORDER BY num_visits DESC
        """).show()

if __name__ == '__main__':
    input_files = sys.argv[1]
    main(input_files)
