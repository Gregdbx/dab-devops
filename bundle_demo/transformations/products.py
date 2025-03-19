from pyspark.sql.functions import col, round, expr

def transform(df):
    """
    Transforme un DataFrame de produits en ajoutant des colonnes calculées
    
    Args:
        df: DataFrame Spark contenant les données produit avec au moins 
           les colonnes 'price' et 'category'
    
    Returns:
        DataFrame: Le DataFrame transformé avec des colonnes supplémentaires
    """
    # Vérifier que les colonnes requises existent
    required_columns = ['price', 'category']
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        raise ValueError(f"Colonnes manquantes dans le DataFrame: {missing}")
    
    # Transformer le DataFrame
    transformed_df = df.withColumn(
        "price_with_tax", round(col("price") * 1.2, 2)
    ).withColumn(
        "price_category", 
        expr("""
            CASE 
                WHEN price < 50 THEN 'Budget'
                WHEN price >= 50 AND price < 200 THEN 'Standard'
                ELSE 'Premium'
            END
        """)
    )
    
    # Calculer une colonne supplémentaire basée sur la catégorie
    transformed_df = transformed_df.withColumn(
        "is_electronic", 
        col("category").isin(["Electronics", "Computers", "Accessories"])
    )
    
    return transformed_df
