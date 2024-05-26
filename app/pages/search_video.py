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

@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

conn = init_connection()

@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
        

title, logo = st.columns([4,2.91])
with title: 
    st.title("YouTube RecoMaster")
with logo: 
    st.write("")
    st.image(icon, width=70)
    
st.slider("Size")
video_name = st.text_input("Enter a video name")
st.write(f"You entered: {video_name}")


data = run_query(
    f"""
        SELECT DISTINCT 
            video_id,
            title, 
            channeltitle,
            thumbnail_link,
            link_video,
            categoryname,
            view
        FROM youtube_trending.search_information
        WHERE title LIKE '%{video_name}%'
        LIMIT 10;
    """
)

videos = {
    "video_id": [e[0] for e in data],
    "title": [e[1] for e in data],
    "channeltitle": [e[2] for e in data],
    "thumbnail_link": [e[3] for e in data],
    "link_video": [e[4] for e in data],
    "categoryname": [e[5] for e in data],
    "view_count": [e[6] for e in data]
}                      
video_url = "https://www.youtube.com/embed/J78aPJ3VyNs"

recommended_videos = []
recommended_videos += videos['link_video']

st.subheader(f"Have {len(videos['video_id'])} results for keyword: {video_name}")
for video_id,title,channeltitle,thumbnail_link,link_video,categoryname,view_count in zip(
    videos['video_id'],videos['title'],videos['channeltitle'],
    videos['thumbnail_link'],videos['link_video'],videos['categoryname'],videos['view_count']):
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        img = Image.open(BytesIO(requests.get(thumbnail_link).content))
        st.markdown(
            f'<style>img {{border-radius: 12px;}}</style>',
            unsafe_allow_html=True,
        )
        st.image(img, use_column_width=True)
    
    with col2:
        st.write("")
        st.markdown(f"""
            <div style="line-height: 1.5;">
                <span style="font-weight: bold;">{title}</span><br>
                <span style="opacity: 0.6;">channel: {channeltitle}</span><br>
                <span style="opacity: 0.6;">category: {categoryname}</span><br>
                <span style="opacity: 0.6;">views: {view_count}</span>
            </div>
            """, unsafe_allow_html=True)
        st.write("")
        is_clicked = st.button("Watch", key=video_id)
        
        if is_clicked:
            st.experimental_set_query_params(video_id=video_id)
            # st.experimental_rerun()
            st.switch_page("./pages/video_detail.py")
            

    st.write("---")


# df = pl.DataFrame(videos)
# st.table(df)