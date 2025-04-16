import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


df=pd.read_csv("amazon.csv")
print(df)
print("initial shape: ", df.shape)
print(df.info())
df.rename(columns={
    'rank':'product_rank'
},inplace=True)

df.drop_duplicates(inplace=True)

df.columns = df.columns.str.strip()

df['product_price'] = df['product_price'].replace('[\$,]','', regex=True)
df['product_price'] = pd.to_numeric(df['product_price'],errors='coerce')

df['product_price'].fillna(df['product_price'].mean(), inplace = True)
df['product_star_rating'].fillna(df['product_star_rating'].mean(), inplace=True)
df['product_num_ratings'].fillna(0, inplace=True)
#print(df)
print(df.isnull().sum())

# Establish connection
con = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "charan@22",
    database = "amazon_software"
)
#check if connected
if con.is_connected():
    print("connected to mysql database!")
cursor=con.cursor()

# 3. Create insert query
insert_query = """
INSERT INTO amazon_data (
    product_title, product_price, product_star_rating,
    product_num_ratings, product_rank, country
)
VALUES (%s, %s, %s, %s, %s, %s)
"""
data_to_insert=[
    (
        row['product_title'],
        float(row['product_price']),
        float(row['product_star_rating']),
        float(row['product_num_ratings']),
        float(row['product_rank']),
        row['country']
    )
    for _, row in df.iterrows()
]
# 4. Loop through dataframe and insert row by row
cursor.executemany(insert_query,data_to_insert)

# 5. Commit and close
con.commit()
print(f"{cursor.rowcount} records inserted.")
cursor.close()
con.close()

# 3. Reload from MySQL for analysis
con = mysql.connector.connect(
    host="localhost",
    user="root",
    password="charan@22",
    database="amazon_software"
)
df = pd.read_sql("SELECT * FROM amazon_data", con)
con.close()
print("Data loaded for analytical computations.")

# print top rated
"""top_rated = df.sort_values(by='product_star_rating',ascending=False).head(10)
print("\n top 10 rated products: ")
print(top_rated[['product_title','product_star_rating']])

# price distribution
plt.figure(figsize=(10, 5))
sns.histplot(df['product_price'], bins=30, kde=True, color='skyblue')
plt.title("Price Distribution")
plt.xlabel("Price ($)")
plt.ylabel("Number of Products")
plt.tight_layout()
plt.show()"""

# Most Reviewed Products
most_reviewed = df.sort_values(by='product_num_ratings', ascending=False).head(10)
print("\nTop 10 Most Reviewed Products:")
print(most_reviewed[['product_title', 'product_num_ratings']])

# Average Ratings and Reviews by Country
country_stats = df.groupby('country').agg({
    'product_star_rating': 'mean',
    'product_num_ratings': 'mean'
}).sort_values(by='product_star_rating', ascending=False)

print("\nAverage Rating and Reviews by Country:")
print(country_stats)

# Free vs Paid Comparison
df['is_free'] = df['product_price'] < 0.5

free_vs_paid = df.groupby('is_free').agg({
    'product_star_rating': 'mean',
    'product_num_ratings': 'mean'
}).reset_index()

free_vs_paid['type'] = free_vs_paid['is_free'].map({True: 'Free', False: 'Paid'})

print("\nFree vs Paid Software Performance:")
print(free_vs_paid[['type', 'product_star_rating', 'product_num_ratings']])

# Plot Free vs Paid Ratings
plt.figure(figsize=(6, 4))
sns.barplot(data=free_vs_paid, x='type', y='product_star_rating', palette='Set2')
plt.title("Average Rating: Free vs Paid Software")
plt.ylabel("Star Rating")
plt.tight_layout()
plt.show()

# top 10 best selling software
print("\nTop 10 Best-Selling Software by Star Rating:")
top_10 = df.sort_values(by='product_star_rating', ascending=False)[
    ['product_title', 'product_star_rating', 'product_num_ratings']
].head(10)
print(top_10.to_string(index=False))

# Average Price of Software per Category
if 'category' in df.columns:
    print("\n Average Software Price per Category:")
    avg_price_category = df.groupby('category')['product_price'].mean().reset_index().sort_values(by='product_price', ascending=False)
    print(avg_price_category.to_string(index=False))
else:
    print("⚠️ 'category' column not found. Skipping average price per category.")

# rating vs review
correlation = df['product_star_rating'].corr(df['product_num_ratings'])
print(f"\n Correlation between Ratings and Review Count: {correlation:.3f}")

                    # or
plt.figure(figsize=(6,4))
sns.scatterplot(x='product_star_rating', y='product_num_ratings', data=df)
plt.title('Rating vs Review Count')
plt.xlabel('Star Rating')
plt.ylabel('Number of Ratings')
plt.tight_layout()
plt.show()

