from scripts.utils.etl import extract, load, truncate, create_spark_session
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql.functions import lit


def handler():

    spark = create_spark_session()

    prop_types = ['pts', 'reb', 'ast', 'stl', 'blk', 'tov']
    output_table = "predicting.predictions_stg"

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

        model_path = f"scripts/predicting/models/{prop_type}_model"
        rf_model = RandomForestRegressionModel.load(model_path)

        # Use the model to predict
        predictions = rf_model.transform(df_features)

        columns_to_keep = ["player_id", "prediction", "matchup"]  # Add other columns if needed
        predictions = predictions.select(*columns_to_keep)

        predictions = predictions.withColumn("prop_type", lit(prop_type))

        load(df=predictions, table=output_table, mode="append")

    spark.stop()