from pyspark.ml.feature import VectorAssembler
from nba_model.utils.etl import extract, load
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def train_model(table_name: str, stat: str, testing: bool, model_path: str):


    input_query = f"SELECT * FROM {table_name}"
    df = extract(input_query)

    df_cleaned = df.na.drop()

    # Select feature columns (all except "pts")
    feature_columns = [col for col in df_cleaned.columns if col != stat]

    # Create a features column
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data = assembler.transform(df_cleaned)

    # Select only features and target columns for ML
    ml_data = data.select("features", stat)

    if testing:
        train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)
    else:
        train_data = ml_data

    # Initialize the model
    rf = RandomForestRegressor(featuresCol="features", labelCol=stat, numTrees=50)

    # Train the model
    rf_model = rf.fit(train_data)

    if testing:
        # Make predictions
        predictions = rf_model.transform(test_data)

        # Evaluate the model
        evaluator = RegressionEvaluator(labelCol=stat, predictionCol="prediction", metricName="mae")
        mae = evaluator.evaluate(predictions)

        print(f"Root Mean Squared Error (RMSE): {mae}")

    else:
        rf_model.save(model_path)
