import pymysql
import pandas as pd 
import numpy as np
from lib2to3.pgen2.token import RARROW
import csv

endpoint = 'll-db-instance.ckrv0jhnsibp.us-east-1.rds.amazonaws.com'
username = 'admin'
password = 'yl8735yl8735'
database_name = 'LL_REC'

connection = pymysql.connect(host= endpoint,user=username,passwd=password,db=database_name)

def lambda_handler(event,context):

    cursor = connection.cursor()
    new_user_id = int(event["user_id"])
    
    if new_user_id<=300 and new_user_id>=1:
        #print("if")
        cursor.execute('SELECT * FROM ll_recommender where userId= "'+ str(new_user_id) +'";')
        row = cursor.fetchall()
        if not row:
            return 'This user has no rating yet........'
        result =  list(row)[0][1:]
        
    else:
        #print("else")
        cursor.execute('SELECT * FROM ll_ratings where user_id = "'+ str(new_user_id) +'";')
        row = cursor.fetchall()
        df_new = pd.DataFrame(row,columns = ['userId','movieId','rating'])
        df_new.reset_index(drop=True)
        
        if len(df_new) == 0:
            return 'This user has no rating yet........'


        df = pd.read_csv("ratings.csv")
        df_new = df_new.rename(columns={'user_id': 'userId', 'movie_id': 'movieId'}) 
        df_res = pd.concat([df,df_new], axis = 0)

        df_res["rating"] = df_res["rating"]/5.0

        #pivot
        movies = df_res.pivot_table(index=['userId'],columns='movieId',values='rating') # x:movie_id; y:user_id
        users = df_res.pivot_table(index=['movieId'],columns='userId',values='rating') # x:user_id; y:movie_id
        
        df_movies = movies.fillna(0) 
        df_users = users.fillna(0)

        userId_list = df_users.columns
        movieId_list = df_movies.columns

        n_movies = len(movieId_list)
        n_users = len(userId_list)
        
        #print("load dataset finished, start to compute...")

        # Compute similarity_matrix_ucf
        new_similarity_matrix_ucf = pd.DataFrame(data = np.zeros((n_users,n_users)), index = userId_list, columns = userId_list)
        user_id = new_user_id
        for other_user_id in userId_list:
            vec_user = []
            vec_other_user_id = []
            if user_id != other_user_id:
                for movie_id in movieId_list:
                    this = df_movies[movie_id]
                    vec_user.append(this[user_id])
                    vec_other_user_id.append(this[other_user_id])
                new_similarity_matrix_ucf[new_user_id][other_user_id] = np.corrcoef(np.array(vec_user), np.array(vec_other_user_id))[0][1]

        similarity_matrix_ucf = pd.read_csv("similarity_matrix_ucf.csv", index_col=0)

        # create a new similarity_matrix_ucf
        similarity_matrix_ucf = pd.concat([similarity_matrix_ucf,new_similarity_matrix_ucf[user_id]], axis = 1)
        similarity_matrix_ucf.iloc[-1] = new_similarity_matrix_ucf[user_id]
        # change column name type to string
        similarity_matrix_ucf.columns = similarity_matrix_ucf.columns.astype(str)
        #print("new similarity_matrix_ucf finished...")
        
        
        
        # User_Based CF user * movie similarity matrix
        avg = np.mean(df_movies, axis = 1) # each user's avg rate
        sum_similarity_uid = np.sum(similarity_matrix_ucf,axis = 1) # sum[S(uid,otheruid) while otheruid in userId_list]
        df_users_mean = df_users - avg

        core_similarity_ucf = pd.DataFrame(data = np.zeros((n_users,n_movies)), index = userId_list, columns = movieId_list)
        uid = new_user_id
        for mid in movieId_list:
            avg_uid = avg[uid]       
            up = np.sum(df_users_mean.T[mid]*similarity_matrix_ucf[str(uid)])
            down = sum_similarity_uid[uid]
            res = avg_uid + up/down
            core_similarity_ucf[mid][uid] = res
        #print("core_similarity_ucf finished...")



        # User_Based CF recommend
        col_name = []
        for i in range(1,101): 
            col_name.append('m_id'+str(i))
        n_col = len(col_name)

        recommend_table = pd.DataFrame(data = np.zeros((n_users,len(col_name))), index = userId_list, columns = col_name)
        core_similarity_ucf_T = core_similarity_ucf.T

        uid = new_user_id
        line = core_similarity_ucf_T[uid].sort_values(ascending=False).index.tolist()[:n_col]
        for j in range(n_col):
            recommend_table[col_name[j]][uid] = line[j]
        line = recommend_table.iloc[-1] # 针对new_user_id的推荐
        #print("recommender line to new_user_id finished...")

        result = line.tolist()
    
    
    # Prepare for frontend data：
    movie_ids = ''
    for s in list(result):
        if s=='':
            continue
        movie_ids += str(s)+','
    movie_ids = movie_ids[0:-1]
    #print(movie_ids)
    cursor.execute("SELECT ll_movies.movie_id,movie_title,movie_year,movie_genres,movie_imdb,movie_tmdb, CONVERT(TRUNCATE(avg(rating),2),CHAR) as rate FROM ll_movies,ll_ratings where ll_movies.movie_id in ("+movie_ids+") and ll_movies.movie_id=ll_ratings.movie_id group by ll_ratings.movie_id;")
    row = cursor.fetchall()
    movies = list(row)
    
    return movies