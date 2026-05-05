import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("pak_cars_clean.csv")

st.title("PakWheels Car Data Analysis")

df["Brand"] = df["Car Name"].str.split().str[0]

st.header("Top 10 Car Brands")
brand_count = df["Brand"].value_counts().head(10)
st.bar_chart(brand_count)

st.header("Average Price by Brand")
avg_price = df.groupby("Brand")["Price"].mean().sort_values(ascending=False).head(10)
st.bar_chart(avg_price)

st.header("Fuel Type Distribution")
st.write(df["Fuel"].value_counts())

st.header("Transmission Count")
st.bar_chart(df["Transmission"].value_counts())

st.header("Year vs Price Scatter Plot")
fig, ax = plt.subplots()
ax.scatter(df["Year"], df["Price"])
st.pyplot(fig)
