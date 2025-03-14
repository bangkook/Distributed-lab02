import pandas as pd
import numpy as np

# Step 1: Load centroids from text file
def load_centroids(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    centroids = [list(map(float, line.strip().split(','))) for line in lines]
    return np.array(centroids)

# Step 2: Load Iris dataset
def load_iris_dataset(path):
    df = pd.read_csv(path)
    return df

# Step 3: Compute Euclidean distance
def euclidean_distance(a, b):
    return np.sqrt(np.sum((a - b) ** 2))

# Step 4: Assign each data point to the nearest centroid
def assign_clusters(df, centroids):
    feature_columns = ['SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm']
    features = df[feature_columns].values
    cluster_assignments = []

    for point in features:
        distances = [euclidean_distance(point, centroid) for centroid in centroids]
        cluster = np.argmin(distances)
        cluster_assignments.append(cluster)

    df['Cluster'] = cluster_assignments
    return df

# Step 5: Group data by cluster
def group_by_cluster(df):
    clusters = {}
    for cluster_id in sorted(df['Cluster'].unique()):
        clusters[cluster_id] = df[df['Cluster'] == cluster_id]
    return clusters

# === Run the whole pipeline ===
centroids = load_centroids("clusters.txt")
iris_df = load_iris_dataset("iris.csv")
clustered_df = assign_clusters(iris_df, centroids)
clusters = group_by_cluster(clustered_df)

# Output clusters
for cluster_id, cluster_data in clusters.items():
    print(f"\n=== Cluster {cluster_id} ===")
    print(cluster_data[['Id', 'SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm', 'Species']])
