import streamlit as st
import psycopg2
import polars as pl
import pandas as pd
from PIL import Image
from io import BytesIO
import requests

icon = Image.open("./icons/youtube_v2.png", mode="r")

st.set_page_config(
    page_title="YouTube RecoMaster",
    page_icon=icon,
    layout="centered",
    initial_sidebar_state="expanded"
)

title, logo = st.columns([4,2.91])
with title: 
    st.title("YouTube RecoMaster")
with logo: 
    st.write("")
    st.image(icon, width=70)


st.markdown(
            f'''<iframe width="705" height="460" src="https://www.youtube.com/embed/dnjKNmn9cyg?si=VbxTczlH7frtQZPy" title="YouTube video player" frameborder="0" 
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; 
                web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>''', 
            unsafe_allow_html=True
        )