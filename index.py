import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

#Inizio caricando il dataset
df = pd.read_csv('data.csv')
print(df.shape)

#mantengo le istanze che hanno come traffuc Subtype più di 10000 istanze
label_counts = df['Traffic Subtype'].value_counts()
valid_labels = label_counts[label_counts > 10000].index
df_filtered = df[df['Traffic Subtype'].isin(valid_labels)]
df_sampled = df_filtered.sample(frac=0.005, random_state=42)
df_dropped = df[~df['Traffic Subtype'].isin(valid_labels)]

df = pd.concat([df_sampled, df_dropped], ignore_index=True)
print(df.shape)

ax = sns.countplot(x='Label', data=df)
ax.set_title('Distribution of Labels')
ax.set_xlabel('Label')
ax.set_ylabel('Count')
plt.xticks(rotation=45, ha='right')
[ax.annotate(f'{p.get_height()}', (p.get_x() + p.get_width()/2., p.get_height()), 
             ha='center', va='bottom', fontsize=9) for p in ax.patches]
plt.tight_layout(); plt.show()

ax = sns.countplot(x='Traffic Type', data=df)
ax.set_title('Distribution of Traffic Types')
ax.set_xlabel('Traffic Type')
ax.set_ylabel('Count')
plt.xticks(rotation=45, ha='right')
[ax.annotate(f'{p.get_height()}', (p.get_x() + p.get_width()/2., p.get_height()), 
             ha='center', va='bottom', fontsize=9) for p in ax.patches]
plt.tight_layout(); plt.show()


TARGET_VARIABLE = 'Traffic Subtype'
DROP_COLUMNS = ['Flow ID', 'Src IP', 'Src Port', 'Dst IP', 'Dst Port', 'Timestamp']
TARGET_TO_DROP = {'Label': ['Traffic Type', 'Traffic Subtype'],
                  'Traffic Type': ['Label', 'Traffic Subtype'],
                  'Traffic Subtype': ['Label', 'Traffic Type']}

#Elimino le righe che non sono utili per la nostra analisi
df = df.drop(columns=DROP_COLUMNS)

#Rimuovo duplicati e i valori del dizionario
df = df.round(3)
df = df.drop_duplicates()
df = df.drop(columns=TARGET_TO_DROP[TARGET_VARIABLE])

print(df.shape)

