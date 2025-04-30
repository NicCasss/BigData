import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Funzione per rimuovere gli outlier
def outlier_remover(db, selected_features):

    q1 = np.percentile(db[selected_features], 25)
    q3 = np.percentile(db[selected_features], 75)
    step = (q3-q1)*1.5

    outlier_indices = db[(db[selected_features] < q1 - step) | (db[selected_features] > q3 + step)].index

    return list(outlier_indices)

selected_features = [
    "Flow Duration",
    "Total Fwd Packet", "Total Bwd packets",
    "Total Length of Fwd Packet", "Total Length of Bwd Packet",
    "Fwd Packet Length Mean", "Bwd Packet Length Mean",
    "Flow Bytes/s", "Flow Packets/s",
    "Flow IAT Mean",
    "Fwd IAT Mean", "Bwd IAT Mean",
    "Fwd Header Length", "Bwd Header Length",
    "Fwd Packets/s", "Bwd Packets/s",
    "Packet Length Mean",
    "SYN Flag Count", "ACK Flag Count", "FIN Flag Count",
    "Average Packet Size",
    "Fwd Segment Size Avg", "Bwd Segment Size Avg",
    "FWD Init Win Bytes", "Bwd Init Win Bytes",
    "Idle Mean", "Active Mean"
]

db = pd.read_csv('./data.csv')
print(db.shape)
spark = SparkSession.builder.appName("IDS_Pipeline").getOrCreate()

outlier_indices = outlier_remover(db, selected_features)
db = db.drop(outlier_indices).reset_index(drop=True)
print(db.shape)

