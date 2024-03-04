#importing the required libraries
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build


#API key connection
def Api_connect():
    Api_Id="AIzaSyBV6Ed_AKJZQcQAqp_tuAjlC9ZYpsD2I7Q"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data


#get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#get comment information
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information
        
#MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["Youtube_data"]

# upload to MongoDB

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"

#Table creation in sql database for channels
def channels_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="Chethu@99",
            database= "youtube_data",
            port = "5432"
            )
    cursor = mydb.cursor()
    
#if there is any table with name channels in mysql database to delete that table
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

#creating the table with respected columns with names as mentioned below
    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, Views bigint,Total_Videos int,Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels Table alredy created")    

#to fetch the collected channels info from mongodb
    channels_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        channels_list.append(ch_data["channel_information"])
    
#converting the data into dataframe
    df = pd.DataFrame(channels_list)
    
#migrating the values from mongodb to the tables created in the sql database
    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,Channel_Id,Subscription_Count,
                                                    Views,Total_Videos,Channel_Description,Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            
        values =(row['Channel_Name'],row['Channel_Id'],row['Subscription_Count'],row['Views'],
                row['Total_Videos'],row['Channel_Description'],row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            print("Channels values are already inserted")


#Table creation in sql database for videos          
def videos_table():
    
    #mysql connection 
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="Chethu@99",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

#if there is any table with name videos in mysql database to delete that table
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

#creating the table with respected columns with names as mentioned below
    try:
        create_query = '''create table if not exists videos(Channel_Name varchar(150),Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, Title varchar(150), Tags text,Thumbnail varchar(225),
                        Description text, Published_Date timestamp,Duration interval, Views bigint, Likes bigint,
                        Comments int,Favorite_Count int, Definition varchar(10), Caption_Status varchar(50) )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        print("Videos Table alrady created")

#to fetch the collected videos info from mongodb
    videos_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            videos_list.append(vi_data["video_information"][i])
            
#converting data into dataframe     
    df2 = pd.DataFrame(videos_list)
    

#migrating the values from mongodb to the tables created in the sql database
    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videos (Channel_Name,Channel_Id,Video_Id, Title, Tags,Thumbnail,Description, 
                          Published_Date, Duration, Views, Likes,Comments, Favorite_Count, Definition, Caption_Status)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_Name'],row['Channel_Id'],row['Video_Id'],row['Title'],row['Tags'],
                  row['Thumbnail'],row['Description'],row['Published_Date'],row['Duration'],row['Views'],
                  row['Likes'],row['Comments'],row['Favorite_Count'],row['Definition'],row['Caption_Status'])                     
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("videos values already inserted in the table")
        

#Table creation in sql database for comments
def comments_table():
    
    #mysql connection
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="Chethu@99",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

#if there is any table with name comments in mysql database to delete that table
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()
    

#creating the table with respected columns with names as mentioned below
    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,Video_Id varchar(80),
                          Comment_Text text, Comment_Author varchar(150),Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()  
    except:
        print("Comments Table already created")
        

#to fetch the collected comments info from mongodb
    comments_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comments_list.append(com_data["comment_information"][i])

#converting data into dataframe
    df3 = pd.DataFrame(comments_list)



#migrating the values from mongodb to the tables created in the sql database
    for index, row in df3.iterrows():
            insert_query = '''INSERT INTO comments (Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published)
                              VALUES (%s, %s, %s, %s, %s)'''
            values = (
                row['Comment_Id'],row['Video_Id'],row['Comment_Text'],row['Comment_Author'],row['Comment_Published'])
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               print("This comments are already exist in comments table")


# calling all the tables under a single function
def tables():
    channels_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"
    
def show_channels_table():
    channels_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        channels_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(channels_list)
    return channels_table


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table


#streamlit part 

st.set_page_config(layout= "wide")
st.markdown('<p style="text-decoration: underline;">PROJECT BY CHETHAN.B</p>', unsafe_allow_html=True)
st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
st.write("")

#creating a sidebar in streamlit web application
with st.sidebar:
    select= option_menu("Main Menu",["Home", "Data-Harvesting and Migrating","Structured_Data_Display","Data Analysis"])
if select=="Home":
    col1,col2= st.columns(2)
    with col1:
        st.header("SKILLS REQUIRED")
        st.caption("***Python scripting***")
        st.caption("***Data Scrapping***")
        st.caption("***MongoDB***")
        st.caption("***SQL***")
        st.caption("***Streamlit***")
    with col2:
        st.video("C:\\Users\\Admin\\Desktop\\youtube_project\\youtube_-_103984 (720p).mp4")
    col3,col4=st.columns(2)
    with col3:
        st.video("C:\\Users\\Admin\\Desktop\\youtube_project\\youtube123.mp4")
    with col4:
        st.header(":red[YouTube]")
        st.caption("***World's Largest Video Sharing App***")
        st.caption("***Shorts is a New added Feature***")
        st.caption("***Share your videos with friends, family, and the world***")        
        
        
#to fetch and store the info of a channel into mongodb database

if select=="Data-Harvesting and Migrating":    
    channel_id = st.text_input("Please Enter Any Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]

    if st.button("Collect and Store data to MongoDB"):
        for channel in channels:
            ch_ids = []
            db = client["Youtube_data"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = channel_details(channel)
                st.success(output)

#migrate the collected data to SQL database 
    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)

#to show the tables of all the channels, videos, and comments on the dashboard
if select=="Structured_Data_Display":    
    tab1, tab2, tab3= st.tabs(["CHANNELS_INFO", "VIDEO_INFO", "COMMENTS_INFO"])
    with tab1:
        show_channels_table()
    with tab2:
        show_videos_table()
    with tab3:
        show_comments_table()


#query part for the analysis of data
#SQL connection 
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="Chethu@99",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()

#Ten queries
if select=="Data Analysis" :   
    question = st.selectbox(
        'Please Select Your Question',
        ('1. All the videos and the Channel Name',
        '2. Channels with most number of videos',
        '3. 10 most viewed videos',
        '4. Comments in each video',
        '5. Videos with highest likes',
        '6. likes of all videos',
        '7. views of each channel',
        '8. videos published in the year 2022',
        '9. average duration of all videos in each channel',
        '10. videos with highest number of comments'))

#1st query
    if question=='1. All the videos and the Channel Name':
        query1='''select title as videos,channel_name as channelname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=["video title","channel name"])
        st.write(df)
        
#2nd query
    elif question=='2. Channels with most number of videos':
            query2='''select channel_name as channelname,total_videos as no_videos from channels
                    order by total_videos desc'''
            cursor.execute(query2)
            mydb.commit()
            t2=cursor.fetchall()
            df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
            st.write(df2)

#3rd query
    elif question=='3. 10 most viewed videos':
            query3='''select views as views,channel_name as channelname,title as videotitle from videos
                    where views is not null order by views desc limit 10'''
            cursor.execute(query3)
            mydb.commit()
            t3=cursor.fetchall()
            df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
            st.write(df3)
            
#4th query
    elif question=='4. Comments in each video':
            query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
            cursor.execute(query4)
            mydb.commit()
            t4=cursor.fetchall()
            df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
            st.write(df4)

#5th query          
    elif question=='5. Videos with highest likes':
            query5='''select title as videotitle,channel_name as channelname, likes as likecount
                    from videos where likes is not null order by likes desc'''
            cursor.execute(query5)
            mydb.commit()
            t5=cursor.fetchall()
            df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
            st.write(df5)
            
#6th query            
    elif question=='6. likes of all videos':
            query6='''select likes as likecount,title as videotitle from videos'''
            cursor.execute(query6)
            mydb.commit()
            t6=cursor.fetchall()
            df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
            st.write(df6)
            
#7th query            
    elif question=='7. views of each channel':
            query7='''select channel_name as channelname,views as  totalviews from channels'''
            cursor.execute(query7)
            mydb.commit()
            t7=cursor.fetchall()
            df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
            st.write(df7)

#8th query           
    elif question=='8. videos published in the year 2022':
            query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                            where extract(year from published_date)=2022'''
            cursor.execute(query8)
            mydb.commit()
            t8=cursor.fetchall()
            df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
            st.write(df8)
            
#9th query            
    elif question=='9. average duration of all videos in each channel':
            query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
            cursor.execute(query9)
            mydb.commit()
            t9=cursor.fetchall()
            df9=pd.DataFrame(t9,columns=["channelname","averageduration"])  
            
#converting the avg duration and time into string
            T9=[]
            for index,row in df9.iterrows():
                    channel_title=row["channelname"]
                    average_duration=row["averageduration"]
                    average_duration_str=str(average_duration)
                    T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
            df1=pd.DataFrame(T9)
            st.write(df1)

#10th query
    elif question=='10. videos with highest number of comments':
            query10='''select title as videotitle,channel_name as channelname,comments as comments from videos 
                    where comments is not null order by comments desc'''
            cursor.execute(query10)
            mydb.commit()
            t10=cursor.fetchall()
            df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
            st.write(df10)


            

