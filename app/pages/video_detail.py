from PIL import Image
import streamlit as st
import psycopg2
from PIL import Image
from io import BytesIO
import requests


icon = Image.open("./icons/youtube_v2.png", mode="r")

st.set_page_config(
    page_title="Video Recommender",
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
    
def display_video(url, recommended_videos=[]):
    if url not in recommended_videos:
        st.markdown(
            f'''<iframe width="705" height="460" src="https://{url}" title="YouTube video player" frameborder="0" 
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; 
                web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>''', 
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'''<iframe width="355" height="160" src="{url}" title="YouTube video player" frameborder="0" 
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; 
                web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>''', 
            unsafe_allow_html=True
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
    
query_params = st.experimental_get_query_params()
video_id = query_params.get('video_id', [None])[0]

data = run_query(f"""
            select distinct 
                title
                , channeltitle 
                , categoryname
                , view
                , likes
                , dislike
                , publishedat
                , link_video
                , tags
            from youtube_trending.search_information si 
            where video_id = '{video_id}';
    """)

videos = {
    "title": data[0][0],
    "channeltitle": data[0][1],
    "categoryname": data[0][2],
    "view": data[0][3],
    "like": data[0][4],
    "dislike": data[0][5],
    "publishedat": data[0][6],
    "link_video": data[0][7],
    "tags": data[0][8]
}

display_video(videos['link_video'])
st.markdown(f"### {videos['title']}")
view_icon = Image.open("./icons/icons8-view-48.png", mode="r")
like_icon = Image.open("./icons/icons8-like-48.png", mode="r")
dislike_icon = Image.open("./icons/icons8-thumbs-down-skin-type-4-48.png", mode="r")
category_icon = Image.open("./icons/icons8-category-48.png", mode="r")
channel_icon = Image.open("./icons/icons8-channel-48.png", mode="r")
# st.write(f"{videos['tags']}")
st.write(f"<span style='color: #6495ED;'>{videos['tags']}</span>", unsafe_allow_html=True)

title, view, like, dislike, category = st.columns([4,1,1,1,1.3])
with title:
    st.image(channel_icon, width=40)
    st.write(f"{videos['channeltitle']}")
with view:
    st.image(view_icon, width=30)
    st.write(f"{videos['view']}")
with like:
    st.image(like_icon, width=30)
    st.write(f"{videos['like']}")
with dislike:
    st.image(dislike_icon, width=30)
    st.write(f"{videos['dislike']}")
with category:
    st.image(category_icon, width=30)
    st.write(f"{videos['categoryname']}")


st.subheader("Recommended Videos:")
tags = ""
tag_list = videos['tags'].split(' ')
for tag in tag_list: tags += f"tags LIKE '%{tag}%' OR "
tags = tags[:-3]

query = f"""
            select distinct 
                video_id
                , title
                , channeltitle 
                , categoryname
                , view
                , likes
                , dislike
                , publishedat
                , link_video
                , tags
                , thumbnail_link
            from youtube_trending.search_information 
            where (categoryname = '{videos['categoryname']}') AND
                    ({tags}) AND video_id <> '{video_id}'
            limit 10;
    """
data2 = run_query(query)

if data2 is not None:
    videos2 = {
        "video_id": [e[0] for e in data2],
        "title": [e[1] for e in data2],
        "channeltitle": [e[2] for e in data2],
        "categoryname": [e[3] for e in data2],
        "view": [e[4] for e in data2],
        "like": [e[5] for e in data2],
        "dislike": [e[6] for e in data2],
        "publishedat": [e[7] for e in data2],
        "link_video": [e[8] for e in data2],
        "tags": [e[9] for e in data2],
        'thumbnail_link': [e[10] for e in data2]
    }
                        

    recommended_videos = []
    recommended_videos += videos2['link_video']

    for video_id,title,channeltitle,categoryname,view,like,dislike,publishedat,link_video,tags,thumbnail_link in zip(
        videos2['video_id'],videos2['title'],videos2['channeltitle'],videos2['categoryname'],
        videos2['view'],videos2['like'], videos2['dislike'],videos2['publishedat'],
        videos2['link_video'],videos2['tags'],videos2['thumbnail_link']):
        
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
                    <span style="opacity: 0.6;">views: {view}</span>
                </div>
                """, unsafe_allow_html=True)
            st.write("")
            st.button("Detail", key=video_id)

        st.write("---")
        
else: st.write(f"Not found")
