import pandas as pd
import streamlit as st

df = pd.read_csv('temp/matches.csv')

for i, row in df.iterrows():
    st.markdown(row['utcDate'])
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(row['homeTeam.name'])
    col1.image(row['homeTeam.crest'])
    col3.markdown('X')
    col5.markdown(row['awayTeam.name'])
    col5.image(row['awayTeam.crest'])
    col2.markdown(row['score.fullTime.home'])
    col4.markdown(row['score.fullTime.away'])