from PIL import Image
import streamlit as st
import psycopg2


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
            f'''<iframe width="705" height="460" src="{url}" title="YouTube video player" frameborder="0" 
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
                i.title
                , i.channeltitle 
                , v.categoryname
                , m.view
                , m.like
                , m.dislike
                , m.publishedat
                , l.link_video
            from gold.informationvideos i 
                inner join gold.linkvideos l on i.video_id = l.video_id 
                inner join gold.videocategory v on i.categoryid = v.categoryid 
                inner join (
                    SELECT 
                        video_id
                        , MAX(view_count) AS view
                        , MAX(likes) as like
                        , MAX(dislikes) as dislike
                        , MAX(publishedat) as publishedat
                    FROM gold.metricvideos
                    GROUP BY video_id
                ) AS m on i.video_id = m.video_id
            where i.video_id = '{video_id}';
    """)

videos = {
    "title": data[0][0],
    "channeltitle": data[0][1],
    "categoryname": data[0][2],
    "view": data[0][3],
    "like": data[0][4],
    "dislike": data[0][5],
    "publishedat": data[0][6],
    "link_video": data[0][7]
}

display_video(videos['link_video'])
st.markdown(f"### {videos['title']}")
view_icon = Image.open("./icons/icons8-view-48.png", mode="r")
like_icon = Image.open("./icons/icons8-like-48.png", mode="r")
dislike_icon = Image.open("./icons/icons8-thumbs-down-skin-type-4-48.png", mode="r")
category_icon = Image.open("./icons/icons8-category-48.png", mode="r")
channel_icon = Image.open("./icons/icons8-channel-48.png", mode="r")

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

    
# st.markdown(f"""
#     <div style="line-height: 1.5;">
#         <span style="font-weight: bold;">{title}</span><br>
#         <span style="opacity: 0.6;">channel: {videos['channeltitle']}</span><br>
#         <span style="opacity: 0.6;">category: {videos['categoryname']}</span><br>
#         <span style="opacity: 0.6;">{st.image(view_icon, width=30)} {videos['view']}</span>
#     </div>
#     """, unsafe_allow_html=True)


st.subheader("Recommended Videos:")
