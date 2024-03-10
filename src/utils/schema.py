from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, DoubleType, \
    BooleanType


def get_schema(brand: str = None) -> StructType:
    """
    Get the schema for a specified brand, or use the default schema if no brand is provided

    :param brand: The brand for which the schema is requested

    :return: The schema for the specified brand or the default schema
    """

    schema = default_schema  # use default_schema as default as it contains all columns

    brand_schema_dats = ["dats"]
    brand_schema_spar = ["spar"]
    if brand:
        if brand in brand_schema_dats:
            schema = dats_schema
        elif brand in brand_schema_spar:
            schema = spar_schema

    return schema


ensign_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True)
])

placeType_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("longName", StringType(), nullable=True),
    StructField("placeTypeDescription", StringType(), nullable=True)
])

geoCoordinates_schema = StructType([
    StructField("latitude", FloatType(), nullable=True),
    StructField("longitude", FloatType(), nullable=True),
])

address_schema = StructType([
    StructField("streetName", StringType(), nullable=True),
    StructField("houseNumber", StringType(), nullable=True),
    StructField("postalcode", StringType(), nullable=True),
    StructField("cityName", StringType(), nullable=True),
    StructField("countryName", StringType(), nullable=True),
    StructField("countryCode", StringType(), nullable=True)
])

temporaryClosures_schema = StructType([
    StructField("from", StringType(), nullable=True),
    StructField("till", StringType(), nullable=True)
])

placeSearchOpeningHours_schema = StructType([
    StructField("date", StringType(), nullable=True),
    StructField("opens", DoubleType(), nullable=True),
    StructField("closes", DoubleType(), nullable=True),
    StructField("isToday", BooleanType(), nullable=True),
    StructField("isOpenForTheDay", BooleanType(), nullable=True)
])

default_schema = StructType(
    [
        StructField("placeId", IntegerType(), False),
        StructField("ensign", ensign_schema, True),
        StructField("commercialName", StringType(), True),
        StructField("branchId", StringType(), True),
        StructField("sourceStatus", StringType(), True),
        StructField("placeType", placeType_schema, True),
        StructField("sellingPartners", ArrayType(StringType()), True),
        StructField("handoverServices", ArrayType(StringType()), True),
        StructField("geoCoordinates", geoCoordinates_schema, True),
        StructField("address", address_schema, True),
        StructField("moreInfoUrl", StringType(), True),
        StructField("routeUrl", StringType(), True),
        StructField("isActive", BooleanType(), True),
        StructField("placeSearchOpeningHours", ArrayType(placeSearchOpeningHours_schema), True),
        StructField("temporaryClosures", ArrayType(temporaryClosures_schema), True)
    ]
)

spar_schema = StructType(
    [
        StructField("placeId", IntegerType(), False),
        StructField("ensign", ensign_schema, True),
        StructField("commercialName", StringType(), True),
        StructField("branchId", StringType(), True),
        StructField("sourceStatus", StringType(), True),
        StructField("placeType", placeType_schema, True),
        StructField("geoCoordinates", geoCoordinates_schema, True),
        StructField("address", address_schema, True),
        StructField("moreInfoUrl", StringType(), True),
        StructField("routeUrl", StringType(), True),
        StructField("isActive", BooleanType(), True),
        StructField("placeSearchOpeningHours", ArrayType(placeSearchOpeningHours_schema), True),
        StructField("temporaryClosures", ArrayType(temporaryClosures_schema), True)
    ]
)

dats_schema = StructType(
    [
        StructField("placeId", IntegerType(), False),
        StructField("ensign", ensign_schema, True),
        StructField("commercialName", StringType(), True),
        StructField("branchId", StringType(), True),
        StructField("sourceStatus", StringType(), True),
        StructField("placeType", placeType_schema, True),
        StructField("geoCoordinates", geoCoordinates_schema, True),
        StructField("address", address_schema, True),
        StructField("moreInfoUrl", StringType(), True),
        StructField("routeUrl", StringType(), True),
        StructField("isActive", BooleanType(), True),
        StructField("temporaryClosures", ArrayType(temporaryClosures_schema), True)
    ]
)
