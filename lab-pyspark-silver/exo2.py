import re
import time
import pandas as pd
from pyspark.sql.types import StringType

MOTIF = r"([A-Z]{2}) ([0-9]{5})$"

users_bronze = users_bronze.cache()
users_bronze.count()


def extraire_zip(adresse):
    if adresse is None:
        return ""
    correspondance = re.search(MOTIF, adresse)
    return correspondance.group(2) if correspondance else ""


udf_zip = F.udf(extraire_zip, StringType())


@F.pandas_udf(StringType())
def pandas_zip(adresses: pd.Series) -> pd.Series:
    return adresses.str.extract(MOTIF)[1].fillna("")


native_zip = F.regexp_extract("address", MOTIF, 2)

verif = users_bronze.select(
    native_zip.alias("a"), udf_zip("address").alias("b"), pandas_zip("address").alias("c")
)
print("ecarts:", verif.filter((F.col("a") != F.col("b")) | (F.col("a") != F.col("c"))).count())

grand = users_bronze.crossJoin(spark.range(100)).drop("id").cache()
print("lignes:", grand.count())


def chronometrer(nom, colonne):
    mesures = []
    for _ in range(3):
        t0 = time.perf_counter()
        grand.select(colonne.alias("zip")).write.format("noop").mode("overwrite").save()
        mesures.append(time.perf_counter() - t0)
    print(nom, [round(m, 3) for m in mesures])


chronometrer("regexp_extract", native_zip)
chronometrer("udf python", udf_zip("address"))
chronometrer("pandas_udf", pandas_zip("address"))