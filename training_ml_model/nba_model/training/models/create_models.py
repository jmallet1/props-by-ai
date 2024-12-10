from nba_model.utils.training import train_model
from datetime import datetime

testing = False

stats = ["pts", "reb", "ast", "blk", "stl", "tov"]

for stat in stats:

    print(f"Training {stat} model...")

    model_path = f"C:/Users/jakem/wager_wiser/training_ml_model/nba_model/predicting/models/{stat}_model"
    model = train_model(table_name=f"training.player_{stat}", stat=stat, testing=testing, model_path=model_path)
    
    current_time = datetime.now().strftime("%H:%M:%S")
    print("Current Time:", current_time)

