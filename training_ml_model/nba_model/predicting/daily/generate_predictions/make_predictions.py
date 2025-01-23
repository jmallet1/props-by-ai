from training_ml_model.nba_model.utils.etl import extract, load, truncate
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession


def handler():
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load team season data to PostgreSQL") \
        .getOrCreate()

    prop_types = ['pts', 'reb', 'ast', 'stl', 'blk', 'tov']
    output_table = "predicting.predictions"

    truncate(output_table)

    for prop_type in prop_types:

        print(f"Predicting {prop_type}")

        input_table = f"predicting.player_{prop_type}"
        input_query = f"SELECT * FROM {input_table}"

        # Get the testing data
        df = extract(input_query, spark)

        all_columns = df.columns

        # Combine feature columns into a single 'features' vector
        non_feature_columns = ["player_id", "matchup", "label"]  # Add any non-feature columns to this list
        feature_columns = [col for col in all_columns if col not in non_feature_columns]

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df_features = assembler.transform(df)

        model_path = f"C:\\Users\\jakem\\wager_wiser\\training_ml_model\\nba_model\\predicting\\models\\{prop_type}_model"
        rf_model = RandomForestRegressionModel.load(model_path)

        # Use the model to predict
        predictions = rf_model.transform(df_features)

        columns_to_keep = ["player_id", "prediction"]  # Add other columns if needed
        predictions = predictions.select(*columns_to_keep)

        predictions = predictions.withColumn("prop_type", lit(prop_type))

        load(df=predictions, table=output_table, mode="append")

    spark.stop()